# DESIGN.md — trade-offs

Every number here is measured, with the command or `file:line` that produced
it. Paragraphs marked **Draft** are reasoning written from those facts, for
mo to revise into his own words. `TODO(mo)` marks a question only mo can
answer — there is no evidence for it in the repo or the session.

All timestamps are UTC from the ingest container clock / `positions.time`.
Session-local runs on WSL2 against the compose stack — not the VPS.

---

## 1. TimescaleDB vs plain Postgres

### Facts

- Image: `timescale/timescaledb-ha:pg16` — `docker-compose.yml:5`.
  Extensions `timescaledb` and `postgis` — `db/schema.sql:5-6`.
- `positions` is a hypertable on `time` — `db/schema.sql:36`:
  `SELECT create_hypertable('positions', 'time', if_not_exists => TRUE);`
- No surrogate key; `(mmsi, time)` is the natural key — `db/schema.sql:24`.
  Unique index `positions_mmsi_time_uniq ON positions (mmsi, time DESC)` —
  `db/schema.sql:38-40`, commented as the dedupe guard for repeated reports.
  GIST index on `geom` — `db/schema.sql:42-43`.
- Chunk interval **7 days**; one chunk so far, `2026-09-03 → 2026-09-10`,
  uncompressed.

  ```
  docker compose exec -T db psql -U sawari -d sawari -c \
    "SELECT column_name, time_interval FROM timescaledb_information.dimensions WHERE hypertable_name='positions';"
  # time | 7 days
  docker compose exec -T db psql -U sawari -d sawari -c \
    "SELECT chunk_name, range_start, range_end, is_compressed FROM timescaledb_information.chunks WHERE hypertable_name='positions';"
  # _hyper_1_1_chunk | 2026-09-03 00:00:00+00 | 2026-09-10 00:00:00+00 | f
  ```

- Retention: `add_retention_policy('positions', INTERVAL '90 days')` —
  `db/schema.sql:46`; the comment above it (`:45`) reads "Disk survival on a
  small VPS. This is a DESIGN.md entry, not an afterthought."
- Compression: `timescaledb.compress`, `compress_segmentby = 'mmsi'`,
  `compress_orderby = 'time DESC'` — `db/schema.sql:49-53`; policy
  `add_compression_policy('positions', INTERVAL '7 days')` — `:54`.
- Live policy jobs:

  ```
  docker compose exec -T db psql -U sawari -d sawari -c \
    "SELECT proc_name, schedule_interval, config FROM timescaledb_information.jobs WHERE hypertable_name='positions';"
  # policy_retention   | 1 day    | {"drop_after": "90 days", "hypertable_id": 1}
  # policy_compression | 12:00:00 | {"hypertable_id": 1, "compress_after": "7 days"}
  ```

- Measured write rate, North Sea box, current container (recreated
  2026-09-06 05:37:15):

  ```
  docker compose exec -T db psql -U sawari -d sawari -c "
  SELECT count(*) AS rows, count(DISTINCT mmsi) AS vessels, min(time), max(time),
         round(count(*) / (EXTRACT(EPOCH FROM (max(time)-min(time)))/60.0)) AS rows_per_min
  FROM positions WHERE time >= '2026-09-06 05:37:15+00';"
  # 127542 | 8221 | 2026-09-06 05:37:16 | 2026-09-06 06:10:04 | 3889
  ```

  Worldwide box, earlier run: 7,829 rows/min including reconnect gaps —
  `PROGRESS.md` query A.
- On-disk size, uncompressed, both runs combined, and the table/index split:

  ```
  docker compose exec -T db psql -U sawari -d sawari -c \
    "SELECT hypertable_size('positions'), (SELECT count(*) FROM positions);"
  # 76242944 | 343624
  docker compose exec -T db psql -U sawari -d sawari -c "SELECT * FROM hypertable_detailed_size('positions');"
  # table_bytes 33964032 | index_bytes 42262528 | toast_bytes 16384 | total_bytes 76242944
  ```

  **Indexes are 55% of the hypertable** (42.3 MB of 76.2 MB). Bytes per row
  including indexes: 76,242,944 / 343,624 = **222 B/row**.
- Compression ratio: **not measurable yet**. `hypertable_compression_stats`
  returns NULLs; `number_compressed_chunks = 0`. First chunk compresses on
  2026-09-10.

**Draft — why a hypertable and not a plain table.** At 3,889 rows/min a plain
`positions` table with a BRIN index on `time` would take the inserts without
complaint; insert throughput is not the reason. The reason is the third
operation, not the first two: dropping data. `DELETE FROM positions WHERE
time < now() - interval '90 days'` on a table that will hold hundreds of
millions of rows is a vacuum and bloat problem that runs forever. Declarative
partitioning fixes that with `DROP PARTITION`, but then something has to
create next week's partition on schedule — a cron job or `pg_partman`, i.e. a
new moving part. The hypertable is declarative partitioning where chunk
creation, the retention job, and the compression job are all built in, on an
image the stack already runs. So the honest framing is: TimescaleDB was chosen
for retention and compression ergonomics, and the throughput headroom is a
side effect.

**Draft — why `segmentby = 'mmsi'`, `orderby = 'time DESC'`.** The read that
matters for the ETA model is one vessel's track over a time range. Segmenting
by `mmsi` puts each vessel's rows in one compressed segment, ordered by time,
so that read decompresses one segment and stops. What it makes worse is the
other read: "every vessel inside this polygon last month." There is no GIST
index on compressed chunks, so a spatial query over cold data decompresses
everything in range. That is acceptable because the dashboard's spatial reads
live in the last few minutes, which is always in the uncompressed 7-day hot
chunk. The trade is explicit: cold data is laid out for the model, hot data
for the map.

**Draft — why 90 days and 7 days, and whether it fits.** Arithmetic on the
measured constants only:

- 3,889 rows/min × 1,440 = **5,600,160 rows/day**
- Hot window (7 days, uncompressed): 7 × 5,600,160 × 222 B = **8.7 GB**
- Cold window (days 8–90) before compression: 83 × 5,600,160 × 222 B = **103 GB**

The CX22's vendor-listed disk is 40 GB (verify with `df -h` on the VPS; not
measured here). Uncompressed, 90-day retention does not fit — not close. The
retention policy is only real if compression delivers a ratio of at least
103 GB ÷ (whatever disk is allotted to cold positions). Two things make that
plausible rather than hopeful: the 55% index share disappears on compressed
chunks (Timescale replaces per-row btree/GIST with segment metadata), and
`segmentby mmsi` puts near-identical values adjacent, which is what columnar
compression is built for. But "plausible" is not a number. **Measure the
ratio from `hypertable_compression_stats('positions')` on or after
2026-09-10 and write it here.** If it's short, the levers are retention days
and the hot window, in that order.

> TODO(mo): What did the plain-Postgres alternative look like in v1, and what
> specifically broke or hurt? No v1 checkout exists on this machine, so this
> is memory only.

---

## 2. Ingest batching

### Facts

- Buffer flushes on **500 rows or 2.0 seconds**, whichever first:
  `BATCH_MAX_ROWS = 500` — `ingest/worker.py:62`;
  `BATCH_MAX_SECONDS = 2.0` — `:63`; size trigger `:241`; timer trigger in
  `flush_timer` (`:266-269`), polled every 0.5 s (`:268`).
- One transaction per batch — `Ingestor.flush()` `:244`,
  `async with self.pool.acquire() as conn, conn.transaction():` `:251`.
  New MMSIs are upserted into `vessels` first (`:254`, FK safety), then
  `executemany(INSERT_POSITION, batch)` `:258`.
- `INSERT_POSITION` — `ingest/worker.py:74-78`, ending
  `ON CONFLICT (mmsi, time) DO NOTHING` (`:77`); the conflict target is the
  unique index at `db/schema.sql:38-40`.
- `vessels` upserts: `UPSERT_VESSEL_SEEN` `:80-83` (`GREATEST(last_seen, $2)`);
  `UPSERT_VESSEL_STATIC` `:85-98` (`COALESCE(EXCLUDED.x, vessels.x)`).
  Static messages are written one at a time in `handle()`, not batched.
- Redis: every accepted position is `PUBLISH`ed (`:231`) and `HSET` into
  `latest_positions` (`:236`), awaited inline in `handle()` (`:184`) before
  the row reaches the batch buffer.
- `websockets.connect(..., max_queue=2048)` — `:156`. Library default is 32:

  ```
  .venv/bin/python -c "import inspect, websockets; s=inspect.signature(websockets.connect); \
    print({k:v.default for k,v in s.parameters.items() if k in ('ping_interval','ping_timeout','max_queue')})"
  # {'ping_interval': 20, 'ping_timeout': 20, 'max_queue': 32}   (websockets 13.1)
  ```

- Drops are counted by reason, never silently discarded —
  `WORKING-AGREEMENT.md:73-74`. Counter `:106`; reasons: `malformed_json` `:189`, `no_message_type` `:198`,
  `unhandled_<MessageType>` `:217`, every parser `DropReason` `:222`,
  `db_error` `:262` (charged the whole batch). Logged every
  `HEARTBEAT_SECONDS = 60` (`:64`, `:274`).
- Measured drop rate, North Sea box, 45 min into the current container:

  ```
  docker compose logs ingest | grep heartbeat | tail -1
  # 06:22:17 heartbeat received=206072 written=174706 rate=3882/min reconnects=0
  #          drops[bad_mmsi=229, impossible_speed=7, unhandled_SubscriptionConfirmation=1]
  ```

  237 dropped of 206,072 received (0.115%). `db_error` has not appeared in
  any heartbeat this session.
- Worldwide box, 5 min in (`PROGRESS.md`): `received=44978 ...
  drops[bad_mmsi=40, impossible_speed=4, null_island=17, unhandled_SubscriptionConfirmation=2]`.
- `received − written − drops` is not loss: `ShipStaticData` counts as
  received but goes to `vessels`; up to 500 rows sit in the buffer at the
  heartbeat instant — `PROGRESS.md:20-22`.

**Draft — what 500 rows / 2 s actually does at these rates.** Divide the
measured rates by 60: the North Sea box delivers ~65 rows/s, the worldwide
box ~130 rows/s. Reaching 500 rows takes 7.7 s and 3.8 s respectively — both
longer than 2 s. So at every rate this project has seen, **the timer fires
first**: one transaction roughly every 2 s carrying ~130 rows (North Sea) or
~260 rows (worldwide), about 0.5 transactions/s. The 500-row cap is not the
operating point; it's a ceiling on memory and on worst-case latency during a
burst, e.g. the backlog after a reconnect. The dashboard's freshness bound is
therefore ~2 s plus flush time from the DB's point of view — but the
dashboard doesn't read the DB for live positions, it reads the Redis publish,
which happens per message before batching. The numbers to move these would
be per-transaction overhead (`pg_stat_statements` on `INSERT_POSITION`) and
flush wall time; neither has been measured yet.

**Draft — `DO NOTHING`, not `DO UPDATE`.** The unique index comment
(`db/schema.sql:38`) says what a conflict is: the same AIS broadcast decoded
by two receivers and forwarded twice. The payload is the same bits; there is
nothing to update, and `DO UPDATE` would cost a heap write and WAL per
duplicate for zero information. A vessel correcting its position sends a new
report with a new `time_utc`, so it never collides. The one theoretical hole:
`time_utc` is AISStream's receiver-side timestamp at nanosecond resolution,
not the AIS message's own seconds-of-minute field — two genuinely different
reports would have to land at the identical nanosecond to be conflated.
Unmeasured: how often conflicts actually fire. `executemany` doesn't return
row counts; measuring it means inserting with `RETURNING 1` and counting, or
comparing `pg_stat_user_tables.n_tup_ins` against `written`.

**Draft — Redis per message, DB per batch.** The asymmetry is deliberate:
the map wants each position as it arrives, the database wants throughput.
The cost is coupling. Both Redis calls are awaited inline in `handle()`, so a
stalled Redis stalls `handle()`, which stops the `async for raw in ws` loop
consuming. The `websockets` library then buffers up to `max_queue=2048`
frames and, once full, stops reading the socket — TCP backpressure, not a
drop and not a crash. When Redis returns, the backlog drains in order. So the
failure mode is a stream pause, which the heartbeat's `received` would show
as a flat line, not a `drops[...]` entry. Pipelining both calls per batch
would cut ~260 awaits/s at the worldwide rate to a handful; that's the
change to make if the loop ever shows lag, and the reason not to make it now
is that nothing has been measured to show it's needed.

**Draft — on `db_error`, drop the batch.** A failed transaction is counted
against `db_error` (whole batch size) and discarded (`:262`). The
alternative — retry in place — means the buffer grows unbounded while the
database is down, until the process dies of memory, taking the ingest loop
with it. That is the one outcome `WORKING-AGREEMENT.md` forbids ("uptime is
the product"). Losing at most ~2 s of positions per failed transaction,
visibly, is the cheaper failure. Where it's wrong: a multi-minute DB outage
loses every row in it. The mitigation isn't code, it's the metric —
`db_error` is a counter on the heartbeat, and it has read zero for the whole
session. Spill-to-disk is the correct answer if that ever stops being true.

---

## 3. Region selection

### Facts

- Original target: Arabian Gulf / Hormuz / Gulf of Oman + Salalah–Duqm
  (`git show b1d108e:ingest/worker.py`, `BOUNDING_BOXES`).
- Symptom: worker connected and subscribed, wrote zero rows. One-shot probe
  `ops/probe2.py:10-14` tried `worldwide`, `gulf_latlon`, and `gulf_lonlat`
  (both coordinate orderings) — Gulf boxes returned nothing in 25 s,
  worldwide returned ~170 msg/s (`PROGRESS.md:78-79`).
- Region breakdown of the 83,767-row worldwide run — `PROGRESS.md:39-67`:

  | region | positions | % |
  |---|---|---|
  | N Europe | 50,395 | 60.52 |
  | other | 12,182 | 14.63 |
  | Mediterranean | 10,519 | 12.63 |
  | US coasts | 10,171 | 12.21 |
  | Arabian Gulf | 0 | 0.00 |
  | Gulf of Oman | 0 | 0.00 |

- Zero confirmed with a deliberately wide box (12–32°N, 44–62°E) —
  `PROGRESS.md:69-76` → `0`.
- Density inside the N Europe bucket, top 2° cells by distinct vessels —
  `PROGRESS.md:106-139`:

  | lat | lon | positions | vessels |
  |---|---|---|---|
  | 50 | 4 | 12,630 | 2,512 |
  | 52 | 4 | 14,701 | 2,127 |
  | 52 | 8 | 4,142 | 625 |
  | 50 | 2 | 3,139 | 594 |
  | 52 | 6 | 2,602 | 485 |
  | 54 | 10 | 1,428 | 251 |
  | 56 | 10 | 1,650 | 231 |
  | 50 | 6 | 1,029 | 227 |
  | 50 | 0 | 1,289 | 216 |

  Nine of the top fifteen cells fall inside the chosen box (`PROGRESS.md:129`).
  The Baltic/Norwegian cells left out — (54,18), (58,10), (54,12), (58,18),
  (58,4) — total 4,737 positions, 9.4% of the N Europe bucket, 139–192
  vessels each.
- Chosen box: `[[49.0, -2.5], [56.0, 10.0]]` — `ingest/worker.py:53-55`,
  format `[[lat_min, lon_min], [lat_max, lon_max]]` (`:46`). Recorded in
  `WORKING-AGREEMENT.md:27-29`.
- Override: `AIS_BOUNDING_BOXES` env var, JSON, same format —
  `ingest/worker.py:50-59`; passed through in `docker-compose.yml`
  (`ingest.environment`); documented in `.env.example`. The worldwide
  diagnosis used `AIS_BOUNDING_BOXES=[[[-90,-180],[90,180]]]`.
- First run on the box: 4,333 of 4,333 rows inside it in the first 73 s
  (`PROGRESS.md:150`, query B). AISStream filters server-side.
- Ports re-seeded to 12 North Sea / Channel ports — `db/schema.sql:100-116`.
  Coordinates approximate (~0.05°) and unverified against UN/LOCODE;
  `anchorage_radius_m` default 15,000, `berth_radius_m` default 3,000 —
  `db/schema.sql:63-64`, `PROGRESS.md:166-170`.
- Commits: `39777cf` (pivot), `de53c69` (measurements).

**Draft — why zero means "no coverage," not "quiet."** AISStream aggregates
terrestrial receivers run by volunteers; it has data where someone has put an
antenna. The Strait of Hormuz is one of the densest tanker lanes on earth, so
"nobody was transmitting" is not a candidate explanation. Three things rule
out a bug on our side: the same 10.7-minute window that produced zero Gulf
rows produced 83,767 rows elsewhere on the same connection; the probe tried
both `[lat, lon]` and `[lon, lat]` orderings and got nothing from either; and
the confirming box (12–32°N, 44–62°E) is wide enough to swallow any
plausible coordinate mistake. When the worldwide feed is delivering 150
messages a second and a box that size gets none, the receivers aren't there.

**Draft — one box, cut at the Baltic and Norway.** The top two cells alone —
Dover Strait and the Rotterdam approaches — hold 4,639 distinct vessels in
ten minutes. The five Baltic/Norwegian cells left out would have added 9.4%
of the rows for a few hundred more vessels, plus a second set of ports to
seed and verify, plus a second set of approach geometries for the ETA model
to learn. For an ETA model, more ports is not more signal; it's the same
signal spread thinner. One box also keeps the subscription payload a single
line. The measured cost of the cut is known (4,737 positions); the benefit
is a model trained on the densest, most homogeneous approach traffic in the
dataset.

**Draft — what the pivot does to the ETA problem.** What the schema already
commits to: a port call is `approach_at` (first fix inside 15 km) and
`arrival_at` (first fix inside 3 km with `sog < 0.5`) — `db/schema.sql:70-79`.
On the North Sea those radii mean something different than in the Gulf:
Rotterdam's approach is a long dredged channel with pilot boarding well
offshore, and Antwerp is 80 km up the Scheldt, so "inside 15 km of the port
point" may trigger far from anything that looks like arrival. The default
radii were set for Gulf ports and have not been re-examined.

> TODO(mo): Which v1 features assumed Gulf geography (anchorage waits off
> Jebel Ali, Hormuz transit time as a feature, etc.) and no longer apply?
> Should `anchorage_radius_m` / `berth_radius_m` be per-port now?

> TODO(mo): Why stay on AISStream instead of a paid provider with Gulf
> coverage? Name the provider you'd have picked and the constraint that
> ruled it out. (The repo-level constraint is
> `WORKING-AGREEMENT.md:14-15`, no new dependencies without asking — but
> the provider and price are yours.)

---

## 4. Shorter notes

### 4.1 `ping_timeout=60`

- `websockets.connect(AISSTREAM_URL, ping_interval=20, ping_timeout=60,
  max_queue=2048)` — `ingest/worker.py:152-156`, with a comment recording
  the observed failure. Commit `1a334ac`.
- The library defaults are `ping_interval=20, ping_timeout=20` (signature
  check above), so the original code was running the defaults.
- The two drops that prompted it, worldwide box — `PROGRESS.md:85-95`:
  `05:14:07` `keepalive ping timeout; no close frame received` (1011);
  `05:20:31` `no close frame received or sent`. The second happened
  **after** the change (container rebuilt 05:18:38) and is a different
  signature.
- Current container: `reconnects=0` through 45 min (heartbeat 06:22:17).

**Draft.** A keepalive ping timeout means the client sent a ping and no pong
arrived within `ping_timeout`. At ~150 frames/s the pong is one small frame
queued behind a stream of larger ones on the same TCP connection; 20 s was
enough for it to lose that race once in five minutes. Raising to 60 s allows
three ping intervals to be outstanding before giving up. The price is
detection latency: a peer that is genuinely dead now takes up to 60 s to
notice instead of 20 s. That is not data loss in the usual sense — the feed
is live-only, so what's missed during a dead connection is missed at either
setting; 60 s just extends the window by 40 s once per real outage. Against
one spurious reconnect every five minutes across a seven-day soak, that's
the right trade. The second signature (`no close frame received or sent`) is
not a ping timeout: it's the TCP connection ending without a WebSocket close
handshake — the server or the path dropped it. To tell the two apart in
future, log the exception class and `ws.close_code` at the reconnect site
(`ingest/worker.py:174-176`); a 1011 with a reason string is the server
closing deliberately, `None` is the network.

### 4.2 AISStream: one connection per key

- Observed at session start: running a probe script alongside the live
  worker made the two compete for the stream and the worker went silent.
  Recorded `PROGRESS.md:172-177`; procedure is `docker compose stop ingest`
  before any probe. Comment in `.env.example`.
- This is an observed constraint, not a documented API limit — nothing here
  cites AISStream documentation.

**Draft — implications, given the observation holds.** The VPS worker and a
local worker cannot both run on one key, and neither can a local diagnostic
probe while the VPS is live. That makes a second API key for local/staging
work a requirement of the deployment, not a convenience. The experiment that
separates "per key" from "per IP" is cheap: run the probe from the VPS
(different IP, same key) while the local worker is up, then again with a
second key from the same IP.

> TODO(mo): How did you establish this — what exactly did you see in the
> worker log and the probe output? Has the per-IP alternative been ruled out?

### 4.3 Pure parser, and its tests

- `ingest/parser.py` imports only `dataclasses`, `datetime`, `typing` —
  `ingest/parser.py:7-11`. No network, DB, or `now()` — convention
  `WORKING-AGREEMENT.md:59`, `:70-71`. Anything impure belongs in `worker.py`.
- Validation constants `MAX_SOG_KNOTS`, `MMSI_MIN/MAX`, `HEADING_UNAVAILABLE`
  — `ingest/parser.py:14-16`; `DropReason` — `:19-25`; `parse_time`
  truncates AISStream's nanosecond `time_utc` to microseconds — `:55-72`.
- Tests: `tests/test_parser.py`, **35 cases**, no fixtures or mocks:

  ```
  .venv/bin/python -m pytest -q
  # 35 passed
  ```

  Commit `943b110`. CI runs `ruff check .` and `python -m pytest -q` on push
  and PR — `.github/workflows/ci.yml`.
- `worker.py` has no tests.

**Draft.** The parser's whole contract is `dict → (Position | None, reason)`.
With no clock inside it, the timestamp path is deterministic and the tests
can pin the exact nanosecond format AISStream sends; with no I/O, every
`DropReason` is a branch and every branch is a five-line test. That is why
35 tests took one file and no fixtures. The rule is hard rather than a
preference because the moment `now()` or a connection leaks in, every test
needs a fake and the fakes need maintaining — the purity line is where the
cheap tests stop. The gap is real: `worker.py` is untested. Testing the
websocket loop would mean faking `websockets`, `asyncpg`, and Redis at once,
which is three fakes for logic that is mostly "await the next thing." But
two pieces of it don't need that: `Stats.line()` is pure, and `flush()` only
needs a stub with `acquire()`/`transaction()`/`executemany()`. Those are the
next tests to write; the reconnect/backoff loop is what the soak is for.

### 4.4 Windows/WSL executable bit

- Initial commit `b1d108e` created seven files as `100755`:
  `WORKING-AGREEMENT.md`,
  `db/schema.sql`, `docker-compose.yml`, `ingest/parser.py`,
  `ingest/worker.py`, `ops/Dockerfile.ingest`, `requirements.txt`.
- Fixed in `637b43c` ("Drop stray executable bit on Python modules (Windows
  filesystem artifact)") and `d2b2668` ("Drop executable bit on
  non-executable files"). Files created later in the session (`tests/`,
  `.github/`, `.env.example`, `PROGRESS.md`, `requirements-dev.txt`) were
  `100644` from the start.

  ```
  git log --format='--- %h %s' --summary | grep -E '^---|mode change|create mode 100755'
  git ls-files -s | awk '{print $1, $4}'     # all 100644 now
  git config --get core.filemode              # true
  findmnt -n -o SOURCE,FSTYPE -T .            # /dev/sdd ext4
  ```

**Draft — what the facts constrain.** The repo lives on native ext4 with
`core.filemode=true`, and every file created during the session came out
`644`. So the environment as it stands does not produce the bit; whatever
produced it was specific to how the original seven files were created. Why
it matters: with `filemode=true`, every clone on Linux shows a mode diff
until it's fixed, and Docker `COPY` preserves the bit into the image — a
`755` `schema.sql` is harmless but reads as carelessness. What prevents it:
not `.gitattributes` (it doesn't govern modes) and not `core.filemode=false`
(that hides the diff without fixing the tree). The fix is the one already
applied — `git add --chmod=-x` when it appears — and keeping the working tree
on ext4 rather than a `/mnt/c` mount.

> TODO(mo): Where did the `755` actually come from — files authored on a
> `/mnt/c` (drvfs) mount, copied from Windows, or an editor default? The
> session has no evidence either way.
