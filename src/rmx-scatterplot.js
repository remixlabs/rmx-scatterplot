import createScatterplot from "regl-scatterplot";
import { asyncBufferFromUrl, parquetReadObjects } from "hyparquet";

function safeNumber(v) {
  const n =
    typeof v === "number" ? v : typeof v === "string" ? Number(v) : Number.NaN;
  return Number.isFinite(n) ? n : Number.NaN;
}

function hsvToRgb01(h, s, v) {
  const i = Math.floor(h * 6);
  const f = h * 6 - i;
  const p = v * (1 - s);
  const q = v * (1 - f * s);
  const t = v * (1 - (1 - f) * s);
  switch (i % 6) {
    case 0:
      return [v, t, p];
    case 1:
      return [q, v, p];
    case 2:
      return [p, v, t];
    case 3:
      return [p, q, v];
    case 4:
      return [t, p, v];
    case 5:
      return [v, p, q];
    default:
      return [v, t, p];
  }
}

function rgb01ToHex([r, g, b]) {
  const to255 = (x) => Math.max(0, Math.min(255, Math.round(x * 255)));
  const rr = to255(r).toString(16).padStart(2, "0");
  const gg = to255(g).toString(16).padStart(2, "0");
  const bb = to255(b).toString(16).padStart(2, "0");
  return `#${rr}${gg}${bb}`;
}

function pickFirstExistingKey(obj, keys) {
  if (!obj) return null;
  for (const k of keys) {
    if (Object.prototype.hasOwnProperty.call(obj, k)) return k;
  }
  return null;
}

function isUsableClusterValue(v) {
  return v !== undefined && v !== null && String(v).trim() !== "";
}

function chooseBestClusterKey(rows, preferredKey) {
  const candidates = [
    preferredKey,
    "cluster",
    "cluster_id",
    "clusterId",
    "label",
    "topic",
    "topic_id",
    "group",
    "group_id",
    "category",
    "category_id",
  ].filter(Boolean);

  const counts = new Map();
  for (const k of candidates) counts.set(k, 0);

  const N = Math.min(2000, rows.length);
  for (let i = 0; i < N; i++) {
    const r = rows[i];
    if (!r) continue;
    for (const k of candidates) {
      if (isUsableClusterValue(r[k])) counts.set(k, counts.get(k) + 1);
    }
  }

  let bestKey = preferredKey || "cluster";
  let bestCount = counts.get(bestKey) ?? 0;

  for (const [k, c] of counts.entries()) {
    if (c > bestCount) {
      bestKey = k;
      bestCount = c;
    }
  }

  return bestKey;
}
function objectToLines(obj, maxKeys = 40) {
  if (!obj || typeof obj !== "object") return String(obj);
  const keys = Object.keys(obj);
  const shown = keys.slice(0, maxKeys);
  const lines = shown.map((k) => {
    const v = obj[k];
    const sv =
      typeof v === "string"
        ? v.length > 200
          ? v.slice(0, 200) + "…"
          : v
        : typeof v === "number" || typeof v === "boolean"
          ? String(v)
          : v == null
            ? String(v)
            : Array.isArray(v)
              ? `[${v.length} items]`
              : typeof v === "object"
                ? "{…}"
                : String(v);
    return `${k}: ${sv}`;
  });
  if (keys.length > shown.length) lines.push(`… (${keys.length - shown.length} more keys)`);
  return lines.join("\n");
}

class RmxScatterplot extends HTMLElement {
  static get observedAttributes() {
    return ["parquet-url", "x", "y", "point-size", "cluster-id", "selected-cluster-name"];
  }

  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._root = document.createElement("div");
    this._root.id = "root";

    this._canvas = document.createElement("canvas");
    this._canvas.id = "canvas";
    this._tooltip = document.createElement("div");
    this._tooltip.id = "tooltip";
    this._tooltip.style.display = "none";

    const style = document.createElement("style");
    style.textContent = `
      :host { display:block; width:100%; height:100%; min-height:200px; }
      #root { position:relative; width:100%; height:100%; }
      canvas { display:block; width:100%; height:100%; }

      #tooltip {
        position:absolute;
        left:0; top:0;
        transform: translate(10px, 10px);
        max-width:420px;
        max-height:260px;
        overflow:auto;
        pointer-events:none;
        border:1px solid rgba(255,255,255,0.18);
        border-radius:10px;
        background: rgba(0,0,0,0.80);
        backdrop-filter: blur(6px);
        color: rgba(255,255,255,0.92);
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size:12px;
        line-height:1.35;
        padding:10px 12px;
        box-shadow: 0 16px 40px rgba(0,0,0,0.35);
        white-space: pre-wrap;
        word-break: break-word;
      }
    `;

    this.shadowRoot.appendChild(style);
    this.shadowRoot.appendChild(this._root);
    this._root.appendChild(this._canvas);
    this._root.appendChild(this._tooltip);

    this._scatterplot = null;
    this._ro = null;

    this._rows = [];
    this._validRows = [];
    this._lastPoints = [];

    this._parquetUrl = "";
    this._fetchAbort = null;

    this._pointSize = 4;
    this._clusterId = "cluster_id";

    this._xOverride = "projection_x";
    this._yOverride = "projection_y";

    this._selectedClusterName = "";

    this._paletteAppliedKey = "";
    this._lastLegendKey = "";

    this._pendingPoints = null;
    this._drawScheduled = false;
    this._drawInFlight = false;

    this.selectedPoints = [];

    this._inferredX = null;
    this._inferredY = null;

    this._effectiveClusterKey = null;
    this._effectiveXKey = null;
    this._effectiveYKey = null;
    this._clusterToIndices = new Map();
    this._mouse = { x: 0, y: 0 };
    this._hoveredIndex = null;
    this._hoverShowTimer = null;
    this._lastHoverShowAt = 0;

    this._hoverColdDelayMs = 150;
    this._hoverWarmWindowMs = 800;
    this._hoverWarmDelayMs = 0;

    this._onCanvasMouseMove = (e) => {
      const rect = this._canvas.getBoundingClientRect();
      this._mouse.x = e.clientX - rect.left;
      this._mouse.y = e.clientY - rect.top;
      if (this._tooltip.style.display !== "none") {
        this._positionTooltip(this._mouse.x, this._mouse.y);
      }
    };
    this._onCanvasMouseLeave = () => {
      this._hideTooltip();
    };
  }
  ["select-cluster"]() {
    const name = (this._selectedClusterName || "").trim();
    if (!name || !this._scatterplot) return;

    const idxs = this._clusterToIndices.get(name) || [];
    if (!idxs.length) {
      this._scatterplot.deselect?.();
      return;
    }
    this._scatterplot.select?.(idxs);
  }

  get parquetUrl() {
    return this._parquetUrl;
  }
  set parquetUrl(v) {
    const next = (v || "").toString().trim();
    if (next === this._parquetUrl) return;
    this._parquetUrl = next;

    if (next) this._loadAndRedrawFromParquetUrl(next);
    else {
      this._rows = [];
      this._redrawFromData();
    }
  }

  get pointSize() {
    return this._pointSize;
  }
  set pointSize(v) {
    const n = Number(v);
    const next = Number.isFinite(n) ? n : 4;
    if (next === this._pointSize) return;
    this._pointSize = next;
    this._initOrResize(true);
    this._redrawFromData();
  }

  get clusterId() {
    return this._clusterId;
  }
  set clusterId(v) {
    const s = (v || "cluster_id").toString().trim() || "cluster_id";
    if (s === this._clusterId) return;
    this._clusterId = s;
    this._paletteAppliedKey = "";
    this._redrawFromData();
  }

  get x() {
    return this._xOverride;
  }
  set x(v) {
    const s = (v || "").toString().trim();
    const next = s || "";
    if (next === this._xOverride) return;
    this._xOverride = next;
    this._redrawFromData();
  }

  get y() {
    return this._yOverride;
  }
  set y(v) {
    const s = (v || "").toString().trim();
    const next = s || "";
    if (next === this._yOverride) return;
    this._yOverride = next;
    this._redrawFromData();
  }

  get selectedClusterName() {
    return this._selectedClusterName;
  }
  set selectedClusterName(v) {
    const s = (v || "").toString().trim();
    if (s === this._selectedClusterName) return;
    this._selectedClusterName = s;
  }

  connectedCallback() {
    const purl = this.getAttribute("parquet-url");
    if (purl != null) this._parquetUrl = (purl || "").toString().trim();

    const xAttr = this.getAttribute("x");
    if (xAttr != null) this._xOverride = (xAttr || "").toString().trim() || this._xOverride;

    const yAttr = this.getAttribute("y");
    if (yAttr != null) this._yOverride = (yAttr || "").toString().trim() || this._yOverride;

    const ps = this.getAttribute("point-size");
    if (ps != null) {
      const n = Number(ps);
      this._pointSize = Number.isFinite(n) ? n : 4;
    }

    const cid = this.getAttribute("cluster-id");
    if (cid != null) this._clusterId = (cid || "cluster_id").trim() || "cluster_id";

    const scn = this.getAttribute("selected-cluster-name");
    if (scn != null) this._selectedClusterName = (scn || "").toString().trim();

    this._initOrResize(true);

    this._canvas.addEventListener("mousemove", this._onCanvasMouseMove);
    this._canvas.addEventListener("mouseleave", this._onCanvasMouseLeave);

    if (this._parquetUrl) this._loadAndRedrawFromParquetUrl(this._parquetUrl);
    else this._redrawFromData();

    this._ro = new ResizeObserver(() => {
      this._initOrResize(false);
      if (this._lastPoints.length) this._queueDraw(this._lastPoints);
    });
    this._ro.observe(this);
  }

  disconnectedCallback() {
    this._ro?.disconnect();
    this._ro = null;

    this._canvas.removeEventListener("mousemove", this._onCanvasMouseMove);
    this._canvas.removeEventListener("mouseleave", this._onCanvasMouseLeave);

    this._hideTooltip();

    try { this._fetchAbort?.abort(); } catch { }
    this._fetchAbort = null;

    try { this._scatterplot?.destroy?.(); } catch { }
    this._scatterplot = null;
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (oldValue === newValue) return;
    if (name === "parquet-url") this.parquetUrl = newValue;
    else if (name === "x") this.x = newValue;
    else if (name === "y") this.y = newValue;
    else if (name === "point-size") this.pointSize = newValue;
    else if (name === "cluster-id") this.clusterId = newValue;
    else if (name === "selected-cluster-name") this.selectedClusterName = newValue;
  }

  async _loadAndRedrawFromParquetUrl(url) {
    const trimmed = (url || "").toString().trim();
    if (!trimmed) return;

    try { this._fetchAbort?.abort(); } catch { }
    this._fetchAbort = new AbortController();

    try {
      const rows = await this._fetchRowsFromParquetUrl(trimmed, this._fetchAbort.signal);
      if (trimmed !== this._parquetUrl) return;

      this._rows = Array.isArray(rows) ? rows : [];
      this._inferXYKeys();
      this._paletteAppliedKey = "";
      this._lastLegendKey = "";
      this._redrawFromData();
    } catch (e) {
      if (e?.name === "AbortError") return;
      console.warn("rmx-scatterplot: failed to load parquet-url", e);
      this._rows = [];
      this._redrawFromData();
    }
  }

  //CORS issues here
  async _fetchRowsFromParquetUrl(url, signal) {
    const file = await asyncBufferFromUrl({
      url,
      requestInit: {
        signal,
        credentials: "omit",
        mode: "cors",
        cache: "no-store",
      },
    });

    return await parquetReadObjects({ file });
  }

  _inferXYKeys() {
    this._inferredX = null;
    this._inferredY = null;

    const first = this._rows && this._rows.length ? this._rows[0] : null;
    if (!first) return;

    const xCandidates = ["x", "projection_x", "umap_x", "tsne_x", "pca_x", "x0"];
    const yCandidates = ["y", "projection_y", "umap_y", "tsne_y", "pca_y", "y0"];

    const xKey = pickFirstExistingKey(first, xCandidates);
    const yKey = pickFirstExistingKey(first, yCandidates);

    if (xKey && yKey) {
      this._inferredX = xKey;
      this._inferredY = yKey;
      return;
    }

    const scanCount = Math.min(25, this._rows.length);
    for (let i = 0; i < scanCount; i++) {
      const r = this._rows[i];
      const xk = pickFirstExistingKey(r, xCandidates);
      const yk = pickFirstExistingKey(r, yCandidates);
      if (xk && yk) {
        this._inferredX = xk;
        this._inferredY = yk;
        return;
      }
    }

    this._inferredX = "x";
    this._inferredY = "y";
  }

  _initOrResize(forceRecreate) {
    const rect = this.getBoundingClientRect();
    const cssW = Math.max(1, rect.width);
    const cssH = Math.max(1, rect.height);
    const dpr = window.devicePixelRatio || 1;

    this._canvas.style.width = "100%";
    this._canvas.style.height = "100%";
    this._canvas.width = Math.max(1, Math.floor(cssW * dpr));
    this._canvas.height = Math.max(1, Math.floor(cssH * dpr));

    const opts = {
      canvas: this._canvas,
      width: cssW,
      height: cssH,
      pixelRatio: dpr,
      pointSize: Math.max(1, Number.isFinite(this._pointSize) ? this._pointSize : 4),
      lassoMinDelay: 0,
    };

    if (!this._scatterplot || forceRecreate) {
      try { this._scatterplot?.destroy?.(); } catch { }
      this._scatterplot = createScatterplot(opts);

      this._scatterplot.set({ colorBy: "valueA" });

      this._wireScatterplotEvents();
      this._wireHoverEvents();
    } else {
      this._scatterplot.set({
        width: cssW,
        height: cssH,
        pixelRatio: dpr,
      });
    }
  }

  _wireScatterplotEvents() {
    const sp = this._scatterplot;
    if (!sp || typeof sp.subscribe !== "function") return;

    sp.subscribe("select", (evt) => {
      const raw = evt?.points;

      let ids = [];
      if (raw && typeof raw.length === "number") {
        ids = Array.from(raw);
      } else if (raw && typeof raw[Symbol.iterator] === "function") {
        ids = Array.from(raw);
      }

      const rows = ids.map((idx) => this._validRows[idx]).filter(Boolean);

      this.selectedPoints = rows;

      this.dispatchEvent(
        new CustomEvent("selected-points", {
          detail: rows,
          bubbles: true,
          composed: true,
        })
      );

      if (rows.length === 1) {
        this.dispatchEvent(
          new CustomEvent("selected-point", {
            bubbles: true,
            composed: true,
          })
        );
      }
    });

    sp.subscribe("deselect", () => {
      this.selectedPoints = [];
      this.dispatchEvent(
        new CustomEvent("selected-points", {
          detail: [],
          bubbles: true,
          composed: true,
        })
      );
    });
  }

  _wireHoverEvents() {
    const sp = this._scatterplot;
    if (!sp || typeof sp.subscribe !== "function") return;

    const onHoverIndex = (idxMaybe) => {
      const idx = Number.isFinite(idxMaybe) ? idxMaybe : null;
      if (idx == null || idx < 0 || idx >= this._validRows.length) {
        this._hoveredIndex = null;
        this._hideTooltip();
        return;
      }
      if (this._hoveredIndex === idx) return;

      this._hoveredIndex = idx;

      const now = Date.now();
      const warm = now - this._lastHoverShowAt <= this._hoverWarmWindowMs;
      const delay = warm ? this._hoverWarmDelayMs : this._hoverColdDelayMs;

      clearTimeout(this._hoverShowTimer);
      this._hoverShowTimer = setTimeout(() => {
        this._showTooltipForIndex(idx);
        this._lastHoverShowAt = Date.now();
      }, delay);
    };

    sp.subscribe("pointover", (e) => {
      const idx = e?.index ?? e?.point ?? e;
      onHoverIndex(idx);
    });

    sp.subscribe("pointout", () => {
      this._hoveredIndex = null;
      this._hideTooltip();
    });

    sp.subscribe("hover", (e) => {
      const idx = e?.index ?? e?.point ?? e;
      if (idx == null) return;
      onHoverIndex(idx);
    });

    sp.subscribe("mousemove", (e) => {
      const idx = e?.index ?? e?.point ?? e;
      if (idx == null) return;
      onHoverIndex(idx);
    });
  }

  _positionTooltip(x, y) {
    const pad = 12;
    const rootRect = this._root.getBoundingClientRect();
    const maxX = rootRect.width;
    const maxY = rootRect.height;

    const t = this._tooltip;
    const w = t.offsetWidth || 320;
    const h = t.offsetHeight || 140;

    let left = x + 14;
    let top = y + 14;

    if (left + w + pad > maxX) left = Math.max(pad, x - w - 14);
    if (top + h + pad > maxY) top = Math.max(pad, y - h - 14);

    t.style.left = `${left}px`;
    t.style.top = `${top}px`;
    t.style.transform = "translate(0, 0)";
  }

  _showTooltipForIndex(idx) {
    const row = this._validRows[idx];
    if (!row) return;

    this._tooltip.textContent = objectToLines(row, 60);
    this._tooltip.style.display = "block";
    this._positionTooltip(this._mouse.x, this._mouse.y);
  }

  _hideTooltip() {
    clearTimeout(this._hoverShowTimer);
    this._hoverShowTimer = null;
    this._tooltip.style.display = "none";
  }

  _redrawFromData() {
    if (!this._scatterplot) return;

    const rows = Array.isArray(this._rows) ? this._rows : [];
    this._validRows = [];
    this._clusterToIndices.clear();

    if (!this._inferredX || !this._inferredY) this._inferXYKeys();

    const first = rows[0] || null;

    const xOverride = (this._xOverride || "").trim();
    const yOverride = (this._yOverride || "").trim();

    const inferredX = this._inferredX || "x";
    const inferredY = this._inferredY || "y";

    const xKey =
      xOverride && first && Object.prototype.hasOwnProperty.call(first, xOverride)
        ? xOverride
        : inferredX;

    const yKey =
      yOverride && first && Object.prototype.hasOwnProperty.call(first, yOverride)
        ? yOverride
        : inferredY;

    this._effectiveXKey = xKey;
    this._effectiveYKey = yKey;

    const requestedClusterKey = this._clusterId || "cluster_id";
    const clusterKey = chooseBestClusterKey(rows, requestedClusterKey);
    this._effectiveClusterKey = clusterKey;

    const clusterVals = rows.map((r) => r?.[clusterKey]);
    const uniqClusterKeys = Array.from(
      new Set(clusterVals.filter(isUsableClusterValue).map((v) => String(v)))
    );
    if (uniqClusterKeys.length === 0) uniqClusterKeys.push("0");

    const clusterToIndex = new Map();
    uniqClusterKeys.forEach((k, i) => clusterToIndex.set(k, i));

    const palette = uniqClusterKeys.map((_, idx) => {
      const h = uniqClusterKeys.length <= 1 ? 0 : idx / uniqClusterKeys.length;
      return rgb01ToHex(hsvToRgb01(h, 0.55, 0.95));
    });

    const paletteKey = `cluster:${clusterKey}:k=${uniqClusterKeys.length}:n=${rows.length}`;
    if (paletteKey !== this._paletteAppliedKey) {
      this._scatterplot.set({
        colorBy: "valueA",
        pointColor: palette,
      });
      this._paletteAppliedKey = paletteKey;
    }

    const counts = new Map();
    for (const k of uniqClusterKeys) counts.set(k, 0);

    const pts = [];
    for (const r of rows) {
      const x = safeNumber(r?.[xKey]);
      const y = safeNumber(r?.[yKey]);
      if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

      const rawCluster = r?.[clusterKey];
      const clusterStr = isUsableClusterValue(rawCluster) ? String(rawCluster) : uniqClusterKeys[0];
      const clusterIdx = clusterToIndex.has(clusterStr) ? clusterToIndex.get(clusterStr) : 0;

      const vi = this._validRows.length;
      this._validRows.push(r);

      if (!this._clusterToIndices.has(clusterStr)) this._clusterToIndices.set(clusterStr, []);
      this._clusterToIndices.get(clusterStr).push(vi);

      counts.set(clusterStr, (counts.get(clusterStr) || 0) + 1);

      pts.push([x, y, clusterIdx]);
    }

    this._lastPoints = pts;
    this._queueDraw(pts);

    const legendPayload = uniqClusterKeys.map((name, i) => ({
      name,
      color: palette[i],
      count: counts.get(name) || 0,
    }));

    const legendKey = `${paletteKey}|legend:${legendPayload.length}|${legendPayload.map((c) => `${c.name}:${c.count}`).join(",")}`;
    if (legendKey !== this._lastLegendKey) {
      this._lastLegendKey = legendKey;
      this.dispatchEvent(
        new CustomEvent("clusters-changed", {
          detail: legendPayload,
          bubbles: true,
          composed: true,
        })
      );
    }
  }

  _queueDraw(points) {
    this._pendingPoints = points;
    if (this._drawScheduled) return;
    this._drawScheduled = true;

    queueMicrotask(async () => {
      this._drawScheduled = false;
      if (this._drawInFlight) return;
      this._drawInFlight = true;

      try {
        const pts = this._pendingPoints || [];
        this._pendingPoints = null;
        await this._scatterplot.draw(pts);
      } finally {
        this._drawInFlight = false;
      }
    });
  }
}

customElements.define("rmx-scatterplot", RmxScatterplot);
