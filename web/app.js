"use strict";

// ─── Config ───────────────────────────────────────────────
const API_BASE = "";
const WS_URL = `${location.protocol === "https:" ? "wss" : "ws"}://${location.host}/ws/positions`;
const ACTIVE_MINUTES = 10;      // matches /vessels?minutes= and the marker prune window
const LIST_LIMIT = 500;         // the API's page cap
const TABLE_ROWS = 100;         // v1 showed 100
const REFRESH_MS = 30000;       // v1 polled every 30 s as backup
const RECONNECT_MS = 5000;      // v1 reconnected after 5 s
const STALE_MS = ACTIVE_MINUTES * 60 * 1000;
const PRUNE_MS = 60000;
const RATE_WINDOW_MS = 5000;
const STOPPED_KN = 0.5;         // the port-call detector's "stopped" threshold
const MAP_CENTER = [52.3, 3.8];
const MAP_ZOOM = 7;

// ─── Helpers ──────────────────────────────────────────────
const $ = (id) => document.getElementById(id);

function setText(id, text) {
  $(id).textContent = text;
}

function setClass(id, cls) {
  const el = $(id);
  el.classList.remove("ok", "warn", "bad");
  if (cls) el.classList.add(cls);
}

const fmtKn = (v) => (v == null ? "—" : `${Number(v).toFixed(1)} kn`);
const fmtDeg = (v) => (v == null ? "—" : `${Math.round(v)}°`);
const fmtPos = (lat, lon) =>
  `${Math.abs(lat).toFixed(3)}°${lat >= 0 ? "N" : "S"} ${Math.abs(lon).toFixed(3)}°${lon >= 0 ? "E" : "W"}`;

// The API emits microsecond timestamps; Date() only promises millisecond
// parsing, so trim to three fractional digits before parsing.
function parseTime(iso) {
  return new Date(String(iso).replace(/(\.\d{3})\d+/, "$1")).getTime();
}

function timeAgo(iso) {
  const s = Math.max(0, Math.floor((Date.now() - parseTime(iso)) / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  return m < 60 ? `${m}m ago` : `${Math.floor(m / 60)}h ago`;
}

// AIS ship type code -> the word v1 showed in its Type column.
function shipClass(code) {
  if (code == null) return "unknown";
  if (code >= 70 && code <= 79) return "cargo";
  if (code >= 80 && code <= 89) return "tanker";
  if (code >= 60 && code <= 69) return "passenger";
  if (code >= 40 && code <= 49) return "hsc";
  if (code === 30) return "fishing";
  if (code === 31 || code === 32 || code === 52) return "tug";
  if (code === 36 || code === 37) return "pleasure";
  if (code === 50) return "pilot";
  return "other";
}

// v1 coloured by status: approaching amber, berthed green.
const markerColor = (sog) => (sog != null && sog >= STOPPED_KN ? "#ffd43b" : "#51cf66");

async function apiFetch(endpoint) {
  try {
    const res = await fetch(`${API_BASE}${endpoint}`);
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return await res.json();
  } catch (err) {
    console.error(`API error [${endpoint}]:`, err);
    return null;
  }
}

// ─── State ────────────────────────────────────────────────
const state = {
  vessels: new Map(),     // mmsi -> latest known fields (REST + socket merged)
  markers: new Map(),     // mmsi -> L.CircleMarker
  pending: new Map(),     // mmsi -> latest socket frame; last write per vessel wins
  selected: null,         // mmsi
  track: null,            // L.Polyline
  ws: null,
  wsLive: false,
  wsEverConnected: false,
  frames: 0,
  applied: 0,
  frameTimes: [],         // performance.now() of recent frames, for frames/s
  apiOk: null,            // null = checking, true, false
  health: null,
  lastUpdate: null,
};

// ─── Map ──────────────────────────────────────────────────
const map = L.map("map").setView(MAP_CENTER, MAP_ZOOM);
L.tileLayer("https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png", {
  attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a>',
  subdomains: "abcd",
  maxZoom: 19,
}).addTo(map);
const canvas = L.canvas({ padding: 0.5 });

function mergeVessel(partial) {
  const mmsi = partial.mmsi;
  const cur = state.vessels.get(mmsi) || { mmsi };
  for (const [k, v] of Object.entries(partial)) {
    if (v !== undefined) cur[k] = v;
  }
  state.vessels.set(mmsi, cur);
  return cur;
}

function upsertMarker(v) {
  if (typeof v.lat !== "number" || typeof v.lon !== "number") return;
  const color = markerColor(v.sog);
  let m = state.markers.get(v.mmsi);
  if (m) {
    m.setLatLng([v.lat, v.lon]);
    if (m.options.fillColor !== color) m.setStyle({ color, fillColor: color });
    return;
  }
  m = L.circleMarker([v.lat, v.lon], {
    renderer: canvas, radius: 3, weight: 1, color, fillColor: color, fillOpacity: 0.9, opacity: 0.9,
  });
  m.on("click", () => selectVessel(v.mmsi));
  m.addTo(map);
  state.markers.set(v.mmsi, m);
}

// One drain per animation frame. The socket writes into `pending`; if the map
// falls behind, later frames for the same vessel overwrite earlier ones, so
// the backlog is bounded by the number of vessels, never by message rate.
function drain() {
  if (state.pending.size > 0) {
    for (const frame of state.pending.values()) {
      upsertMarker(mergeVessel(frame));
      state.applied += 1;
    }
    state.pending.clear();
    state.lastUpdate = new Date();
    if (state.selected != null && state.vessels.has(state.selected)) {
      renderDrawer(state.vessels.get(state.selected));
    }
  }
  requestAnimationFrame(drain);
}

// ─── WebSocket ────────────────────────────────────────────
function setLive(live) {
  state.wsLive = live;
  const pill = $("live-pill");
  pill.classList.toggle("live", live);
  pill.classList.toggle("down", !live && state.wsEverConnected);
  setText("live-label", live ? "LIVE" : state.wsEverConnected ? "RECONNECTING" : "CONNECTING");
  renderConnection();
}

function connectWebSocket() {
  let ws;
  try {
    ws = new WebSocket(WS_URL);
  } catch (err) {
    console.error("WebSocket error:", err);
    setLive(false);
    setTimeout(connectWebSocket, RECONNECT_MS);
    return;
  }
  ws.onopen = () => {
    state.wsEverConnected = true;
    setLive(true);
  };
  ws.onmessage = (event) => {
    state.frames += 1;
    state.frameTimes.push(performance.now());
    let frame;
    try {
      frame = JSON.parse(event.data);
    } catch {
      return;
    }
    if (frame && typeof frame.mmsi === "number" && typeof frame.lat === "number" && typeof frame.lon === "number") {
      state.pending.set(frame.mmsi, frame);
    }
  };
  ws.onclose = () => {
    setLive(false);
    setTimeout(connectWebSocket, RECONNECT_MS);
  };
  ws.onerror = () => {
    setLive(false);
  };
  state.ws = ws;
}

function framesPerSecond() {
  const cutoff = performance.now() - RATE_WINDOW_MS;
  while (state.frameTimes.length && state.frameTimes[0] < cutoff) state.frameTimes.shift();
  return state.frameTimes.length / (RATE_WINDOW_MS / 1000);
}

// ─── REST ─────────────────────────────────────────────────
function setApi(ok) {
  state.apiOk = ok;
  renderConnection();
}

async function refreshVessels() {
  const data = await apiFetch(`/vessels?minutes=${ACTIVE_MINUTES}&limit=${LIST_LIMIT}`);
  if (!data) {
    setApi(false);
    return;
  }
  setApi(true);
  for (const item of data.items) upsertMarker(mergeVessel(item));
  state.lastUpdate = new Date();
  renderTable();
}

async function refreshHealth() {
  const h = await apiFetch("/health");
  state.health = h;
  setApi(h != null);
  renderKpis();
}

function prune() {
  const now = Date.now();
  for (const [mmsi, v] of state.vessels) {
    if (mmsi === state.selected || !v.time) continue;
    if (now - parseTime(v.time) > STALE_MS) {
      const m = state.markers.get(mmsi);
      if (m) map.removeLayer(m);
      state.markers.delete(mmsi);
      state.vessels.delete(mmsi);
    }
  }
}

// ─── Track view ───────────────────────────────────────────
async function selectVessel(mmsi) {
  state.selected = mmsi;
  renderTable();
  const v = state.vessels.get(mmsi);
  if (v) renderDrawer(v);
  $("drawer").hidden = false;
  renderTrackPanel(mmsi, null, true);

  const track = await apiFetch(`/vessels/${mmsi}/track?hours=24&limit=1000`);
  if (state.selected !== mmsi) return; // user moved on while we waited
  if (state.track) {
    map.removeLayer(state.track);
    state.track = null;
  }
  if (track && track.points.length > 0) {
    state.track = L.polyline(track.points.map((p) => [p.lat, p.lon]), {
      color: "#0ea5e9", weight: 2, opacity: 0.8,
    }).addTo(map);
    map.fitBounds(state.track.getBounds(), { padding: [30, 30], maxZoom: 12 });
  }
  renderTrackPanel(mmsi, track, false);
}

function closeDrawer() {
  state.selected = null;
  if (state.track) {
    map.removeLayer(state.track);
    state.track = null;
  }
  $("drawer").hidden = true;
  renderTrackPanel(null, null, false);
  renderTable();
}

// ─── Rendering ────────────────────────────────────────────
function infoRow(label, value, cls) {
  const row = document.createElement("div");
  row.className = "info-row";
  const l = document.createElement("span");
  l.textContent = label;
  const r = document.createElement("span");
  r.textContent = value;
  if (cls) r.classList.add(cls);
  row.append(l, r);
  return row;
}

function renderTrackPanel(mmsi, track, loading) {
  const rows = $("track-rows");
  rows.replaceChildren();
  if (mmsi == null) {
    const e = document.createElement("div");
    e.className = "empty small";
    e.textContent = "Click a vessel to load its 24 h track";
    rows.append(e);
    return;
  }
  const v = state.vessels.get(mmsi) || { mmsi };
  rows.append(infoRow("Vessel", v.name || `MMSI ${mmsi}`));
  if (loading) {
    rows.append(infoRow("Track", "Loading...", "warn"));
    return;
  }
  if (!track) {
    rows.append(infoRow("Track", "Unavailable", "bad"));
    return;
  }
  const pts = track.points;
  rows.append(infoRow("Points", `${pts.length}${track.capped ? " (capped)" : ""}`, track.capped ? "warn" : null));
  rows.append(infoRow("Window", `${track.hours} h`));
  if (pts.length > 0) {
    rows.append(infoRow("First fix", timeAgo(pts[0].time)));
    rows.append(infoRow("Last fix", timeAgo(pts[pts.length - 1].time)));
  } else {
    rows.append(infoRow("Fixes", "none in window", "warn"));
  }
}

function renderDrawer(v) {
  setText("drawer-name", v.name || `MMSI ${v.mmsi}`);
  const parts = [`MMSI ${v.mmsi}`, shipClass(v.ship_type)];
  if (v.destination) parts.push(v.destination);
  setText("drawer-id", parts.join(" · "));
  setText("d-speed", v.sog != null && v.sog >= STOPPED_KN ? fmtKn(v.sog) : "Stopped");
  setText("d-course", fmtDeg(v.cog));
  setText("d-heading", fmtDeg(v.heading));
  setText("d-position", typeof v.lat === "number" ? fmtPos(v.lat, v.lon) : "—");
  setText("d-seen", v.time ? timeAgo(v.time) : "—");
}

function cell(text, cls) {
  const td = document.createElement("td");
  if (cls) td.className = cls;
  td.textContent = text;
  return td;
}

function renderTable() {
  const body = $("vessel-rows");
  const vessels = [...state.vessels.values()]
    .filter((v) => v.time)
    .sort((a, b) => parseTime(b.time) - parseTime(a.time))
    .slice(0, TABLE_ROWS);

  body.replaceChildren();
  if (vessels.length === 0) {
    const tr = document.createElement("tr");
    const td = cell("Waiting for positions...", "empty");
    td.colSpan = 7;
    tr.append(td);
    body.append(tr);
    return;
  }
  for (const v of vessels) {
    const tr = document.createElement("tr");
    if (v.mmsi === state.selected) tr.classList.add("selected");
    tr.addEventListener("click", () => selectVessel(v.mmsi));

    const nameTd = document.createElement("td");
    const name = document.createElement("div");
    name.className = "v-name";
    name.textContent = v.name || `MMSI ${v.mmsi}`;
    const id = document.createElement("div");
    id.className = "v-id";
    id.textContent = `MMSI ${v.mmsi}`;
    nameTd.append(name, id);

    const typeTd = document.createElement("td");
    const tag = document.createElement("span");
    tag.className = "tag";
    tag.textContent = shipClass(v.ship_type);
    typeTd.append(tag);

    tr.append(
      nameTd,
      typeTd,
      cell(v.destination || "—"),
      cell(v.sog != null && v.sog >= STOPPED_KN ? fmtKn(v.sog) : "—", "mono"),
      cell(fmtDeg(v.cog), "mono"),
      cell(fmtDeg(v.heading), "mono"),
      cell(timeAgo(v.time), "mono"),
    );
    body.append(tr);
  }
}

function renderConnection() {
  if (state.apiOk === true) {
    setText("conn-api", "Connected");
    setClass("conn-api", "ok");
  } else if (state.apiOk === false) {
    setText("conn-api", "Disconnected");
    setClass("conn-api", "bad");
  } else {
    setText("conn-api", "Checking...");
    setClass("conn-api", "warn");
  }

  if (state.wsLive) {
    setText("conn-ws", `Live · ${framesPerSecond().toFixed(1)}/s`);
    setClass("conn-ws", "ok");
  } else {
    setText("conn-ws", state.wsEverConnected ? "Reconnecting..." : "Connecting...");
    setClass("conn-ws", state.wsEverConnected ? "bad" : "warn");
  }

  const coalesced = state.frames - state.applied - state.pending.size;
  setText("conn-stream", `${state.frames} rx · ${state.applied} applied · ${Math.max(0, coalesced)} coalesced`);

  const drops = state.health && state.health.stream ? state.health.stream.dropped : null;
  setText("conn-drops", drops == null ? "—" : String(drops));
  setClass("conn-drops", drops == null ? null : drops > 0 ? "warn" : "ok");
}

function renderKpis() {
  let moving = 0;
  for (const v of state.vessels.values()) if (v.sog != null && v.sog >= STOPPED_KN) moving += 1;
  setText("kpi-active", String(state.markers.size));
  setText("kpi-active-sub", `${moving} moving · last ${ACTIVE_MINUTES} min`);

  setText("kpi-rate", state.wsLive ? framesPerSecond().toFixed(1) : "—");
  setText("kpi-rate-sub", `${state.applied} applied · ${Math.max(0, state.frames - state.applied)} coalesced`);

  const h = state.health;
  const rows = h && h.db ? h.db.rows_approx : null;
  setText("kpi-rows", rows == null ? "—" : rows.toLocaleString("en-US"));
  const lastWrite = h && h.db ? h.db.last_write : null;
  setText("kpi-write", lastWrite ? timeAgo(lastWrite) : "—");
  setText("kpi-write-sub", state.lastUpdate ? `Updated ${timeAgo(state.lastUpdate.toISOString())}` : "Waiting for data");
}

function tick() {
  setText("clock", `${new Date().toISOString().slice(11, 19)} UTC`);
  renderKpis();
  renderConnection();
  setText("table-meta", state.lastUpdate ? `Updated ${timeAgo(state.lastUpdate.toISOString())} · ${state.vessels.size} vessels` : "Waiting...");
}

// ─── Init ─────────────────────────────────────────────────
$("drawer-close").addEventListener("click", closeDrawer);

refreshVessels();
refreshHealth();
connectWebSocket();
requestAnimationFrame(drain);

setInterval(refreshVessels, REFRESH_MS);
setInterval(refreshHealth, REFRESH_MS);
setInterval(prune, PRUNE_MS);
setInterval(tick, 1000);
setTimeout(() => map.invalidateSize(), 0);
