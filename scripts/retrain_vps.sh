#!/usr/bin/env bash
# Rebuild port_calls over the full soak history and retrain the ETA model.
# Run on the VPS, from the repo root (or anywhere — the script cd's there).
#
#   ./scripts/retrain_vps.sh
#
# Five steps: positions sanity check, port_calls rebuild (same plausibility
# gate the detector always used), the calls/arrivals report, the baseline
# (ml.eval), then XGBoost (ml.train) — but only if the holdout cleared 30
# calls. ml.eval and ml.train run inside the `ml` compose service (built
# from requirements-ml.txt, i.e. xgboost-cpu included) via
# `docker compose run --rm`, since the VPS has no .venv. Never touches the
# `ingest` container — PROGRESS.md 2026-09-18.
#
# Step 2 used to be the one that got OOM-killed on the full 53M-row soak
# (exit 137 at 2.3 GB then 14.6 GB RSS — PROGRESS.md 2026-09-19); the
# detector now streams one vessel's fixes at a time instead of fetching
# every candidate vessel's history at once, so this run also reports the
# real peak RSS on that dataset — the number DESIGN.md S4.5's TODO(mo)
# is waiting on.
#
# Every `docker compose run` below passes --build: the first two OOM
# reruns on the VPS burned through the memory fix silently, because
# `docker compose run` reuses whatever image already exists and never
# rebuilds on its own — those two runs were executing the stale,
# pre-fix portcalls image. PROGRESS.md 2026-09-19.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== step 1: positions sanity check =="
docker compose exec -T db psql -U sawari -d sawari -c \
  "SELECT count(*), min(time), max(time) FROM positions;"

echo
echo "== step 2: rebuild port_calls over full history (same MAX_KMH=60 gate) =="
SINCE_MIN=$(docker compose exec -T db psql -U sawari -d sawari -tAc \
  "SELECT ceil(EXTRACT(EPOCH FROM (now() - min(time)))/60)::int FROM positions;" | tr -d '[:space:]')
echo "since_minutes=$SINCE_MIN"
docker compose exec -T db psql -U sawari -d sawari -c "TRUNCATE port_calls;"
docker compose run --rm --build -T portcalls python -c "
import resource, subprocess, sys
p = subprocess.run(['python', '-m', 'ml.portcalls_job', '--once', '--since-minutes', '$SINCE_MIN'])
print(f'peak_rss_mb={resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss / 1024:.1f}')
sys.exit(p.returncode)
"

echo
echo "== step 3: calls / arrivals / arrivals with an observed approach phase =="
docker compose exec -T db psql -U sawari -d sawari -c "
SELECT count(*) AS calls,
       count(*) FILTER (WHERE arrival_at IS NOT NULL) AS arrivals,
       count(*) FILTER (WHERE arrival_at IS NOT NULL AND approach_at < arrival_at)
         AS arrivals_with_observed_approach
FROM port_calls;"

echo
echo "== step 4: baseline eval, temporal holdout (default holdout_frac=0.2) =="
EVAL_OUT="$(mktemp)"
trap 'rm -f "$EVAL_OUT"' EXIT
docker compose run --rm --build -T ml python -m ml.eval | tee "$EVAL_OUT"

HOLDOUT_CALLS="$(grep -m1 '== holdout (arrival_at >= cutoff)' "$EVAL_OUT" | grep -oP 'calls=\K[0-9]+' || true)"
if [ -z "$HOLDOUT_CALLS" ]; then
  echo
  echo "Could not parse the holdout call count out of ml.eval's output — stopping before ml.train." >&2
  exit 1
fi
echo
echo "holdout calls: $HOLDOUT_CALLS"

if [ "$HOLDOUT_CALLS" -lt 30 ]; then
  echo "Holdout has $HOLDOUT_CALLS calls, fewer than 30 — stopping before ml.train." >&2
  exit 1
fi

echo
echo "== step 5: XGBoost, defaults, writes model_registry =="
docker compose run --rm --build -T ml python -m ml.train
