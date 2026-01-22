import createScatterplot from "regl-scatterplot";
import { asyncBufferFromUrl, parquetReadObjects } from "hyparquet";

/**
 * Convert arbitrary input into a finite number. Returns NaN when unusable.
 */
function toFiniteNumber(input) {
  if (typeof input === "number") return Number.isFinite(input) ? input : Number.NaN;

  if (typeof input === "string") {
    const parsedNumber = Number(input);
    return Number.isFinite(parsedNumber) ? parsedNumber : Number.NaN;
  }

  return Number.NaN;
}

/**
 * Make a value JSON-serializable.
 */
function jsonSafe(value) {
  if (value === null || value === undefined) return value;

  const t = typeof value;

  if (t === "bigint") {
    return value.toString();
  }

  if (t === "string" || t === "number" || t === "boolean") {
    return value;
  }

  if (Array.isArray(value)) {
    return value.map(jsonSafe);
  }

  if (t === "object") {
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      out[k] = jsonSafe(v);
    }
    return out;
  }

  return undefined;
}

/**
 * HSV (0..1) -> RGB (0..1). Used for deterministic cluster palettes.
 */
function hsvToRgb01(hue01, saturation01, value01) {
  const sectorIndex = Math.floor(hue01 * 6);
  const sectorFraction = hue01 * 6 - sectorIndex;

  const p = value01 * (1 - saturation01);
  const q = value01 * (1 - sectorFraction * saturation01);
  const t = value01 * (1 - (1 - sectorFraction) * saturation01);

  switch (sectorIndex % 6) {
    case 0:
      return [value01, t, p];
    case 1:
      return [q, value01, p];
    case 2:
      return [p, value01, t];
    case 3:
      return [p, q, value01];
    case 4:
      return [t, p, value01];
    case 5:
      return [value01, p, q];
    default:
      return [value01, t, p];
  }
}

function rgb01ToHex(rgb01) {
  const [red01, green01, blue01] = rgb01;

  const toByte = (channel01) =>
    Math.max(0, Math.min(255, Math.round(channel01 * 255)));

  const red = toByte(red01).toString(16).padStart(2, "0");
  const green = toByte(green01).toString(16).padStart(2, "0");
  const blue = toByte(blue01).toString(16).padStart(2, "0");

  return `#${red}${green}${blue}`;
}

function pickFirstExistingKey(object, candidateKeys) {
  if (!object) return null;

  for (const key of candidateKeys) {
    if (Object.prototype.hasOwnProperty.call(object, key)) return key;
  }

  return null;
}

function isUsableClusterValue(clusterValue) {
  return (
    clusterValue !== undefined &&
    clusterValue !== null &&
    String(clusterValue).trim() !== ""
  );
}

/**
 * Choose the best available cluster key by counting presence in a sample of rows.
 */
function chooseBestClusterKey(rows, preferredClusterKey) {
  const candidateClusterKeys = [
    preferredClusterKey,
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

  const presenceCounts = new Map(candidateClusterKeys.map((key) => [key, 0]));
  const sampleSize = Math.min(2000, rows.length);

  for (let rowIndex = 0; rowIndex < sampleSize; rowIndex++) {
    const row = rows[rowIndex];
    if (!row) continue;

    for (const key of candidateClusterKeys) {
      if (isUsableClusterValue(row[key])) {
        presenceCounts.set(key, presenceCounts.get(key) + 1);
      }
    }
  }

  let bestKey = preferredClusterKey || "cluster";
  let bestCount = presenceCounts.get(bestKey) ?? 0;

  for (const [key, count] of presenceCounts.entries()) {
    if (count > bestCount) {
      bestKey = key;
      bestCount = count;
    }
  }

  return bestKey;
}

function formatTooltipLines(rowObject, maxKeys = 40) {
  if (!rowObject || typeof rowObject !== "object") return String(rowObject);

  const keys = Object.keys(rowObject);
  const displayedKeys = keys.slice(0, maxKeys);

  const lines = displayedKeys.map((key) => {
    const value = rowObject[key];

    if (typeof value === "string") {
      const truncated = value.length > 200 ? `${value.slice(0, 200)}…` : value;
      return `${key}: ${truncated}`;
    }

    if (typeof value === "number" || typeof value === "boolean") {
      return `${key}: ${value}`;
    }

    if (value == null) {
      return `${key}: ${value}`;
    }

    if (Array.isArray(value)) {
      return `${key}: [${value.length} items]`;
    }

    if (typeof value === "object") {
      return `${key}: {…}`;
    }

    return `${key}: ${String(value)}`;
  });

  if (keys.length > displayedKeys.length) {
    lines.push(`… (${keys.length - displayedKeys.length} more keys)`);
  }

  return lines.join("\n");
}

function readStringAttribute(element, name, fallback = "") {
  const value = element.getAttribute(name);
  return value == null ? fallback : String(value).trim();
}

function readNumberAttribute(element, name, fallback) {
  const value = element.getAttribute(name);
  if (value == null) return fallback;

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

class RmxScatterplot extends HTMLElement {
  static get observedAttributes() {
    return [
      "parquet-url",
      "x",
      "y",
      "point-size",
      "cluster-id",
      "selected-cluster-name",
    ];
  }

  // DOM
  #root;
  #canvas;
  #tooltip;

  // Scatterplot instance + resizing
  #scatterplot;
  #resizeObserver;

  // Data
  #rows;
  #validRows;
  #lastDrawnPoints;

  // Request control
  #parquetUrl;
  #fetchAbortController;

  // Manifest inputs
  #pointSize;
  #clusterIdInput;
  #xOverride;
  #yOverride;
  #selectedClusterName;

  // Derived keys and indices
  #inferredXKey;
  #inferredYKey;
  #effectiveXKey;
  #effectiveYKey;
  #effectiveClusterKey;
  #clusterToIndices;

  // Palette / legend caching
  #paletteAppliedKey;
  #legendAppliedKey;
  #lastNonEmptyLegendPayload;


  // Draw scheduling
  #pendingPoints;
  #drawScheduled;
  #drawInFlight;

  // Hover state
  #mousePosition;
  #hoveredIndex;
  #hoverShowTimer;
  #lastHoverShowAt;
  #hoverColdDelayMs;
  #hoverWarmWindowMs;
  #hoverWarmDelayMs;

  // Bound handlers
  #onCanvasMouseMove;
  #onCanvasMouseLeave;

  constructor() {
    super();
    this.attachShadow({ mode: "open" });

    this.#root = document.createElement("div");
    this.#root.id = "root";

    this.#canvas = document.createElement("canvas");
    this.#canvas.id = "canvas";

    this.#tooltip = document.createElement("div");
    this.#tooltip.id = "tooltip";
    this.#tooltip.style.display = "none";

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
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono",
          "Courier New", monospace;
        font-size:12px;
        line-height:1.35;
        padding:10px 12px;
        box-shadow: 0 16px 40px rgba(0,0,0,0.35);
        white-space: pre-wrap;
        word-break: break-word;
      }
    `;

    this.shadowRoot.append(style, this.#root);
    this.#root.append(this.#canvas, this.#tooltip);

    this.#scatterplot = null;
    this.#resizeObserver = null;

    this.#rows = [];
    this.#validRows = [];
    this.#lastDrawnPoints = [];

    this.#parquetUrl = "";
    this.#fetchAbortController = null;

    this.#pointSize = 4;
    this.#clusterIdInput = "cluster_id";
    this.#xOverride = "projection_x";
    this.#yOverride = "projection_y";
    this.#selectedClusterName = "";

    this.#inferredXKey = null;
    this.#inferredYKey = null;
    this.#effectiveXKey = null;
    this.#effectiveYKey = null;
    this.#effectiveClusterKey = null;
    this.#clusterToIndices = new Map();

    this.#paletteAppliedKey = "";
    this.#legendAppliedKey = "";

    this.#pendingPoints = null;
    this.#drawScheduled = false;
    this.#drawInFlight = false;

    this.#mousePosition = { x: 0, y: 0 };
    this.#hoveredIndex = null;
    this.#hoverShowTimer = null;
    this.#lastHoverShowAt = 0;

    this.#hoverColdDelayMs = 150;
    this.#hoverWarmWindowMs = 800;
    this.#hoverWarmDelayMs = 0;

    this.#onCanvasMouseMove = (event) => {
      const rect = this.#canvas.getBoundingClientRect();
      this.#mousePosition.x = event.clientX - rect.left;
      this.#mousePosition.y = event.clientY - rect.top;

      if (this.#tooltip.style.display !== "none") {
        this.#positionTooltip(this.#mousePosition.x, this.#mousePosition.y);
      }
    };

    this.#onCanvasMouseLeave = () => {
      this.#hideTooltip();
    };
  }

  /**
   * Manifest in-event: "select-cluster"
   */
  ["select-cluster"]() {
    const clusterName = (this.#selectedClusterName || "").trim();
    if (!clusterName || !this.#scatterplot) return;

    const indices = this.#clusterToIndices.get(clusterName) || [];
    if (indices.length === 0) {
      this.#scatterplot.deselect?.();
      return;
    }

    this.#scatterplot.select?.(indices);
  }

  get parquetUrl() {
    return this.#parquetUrl;
  }
  set parquetUrl(value) {
    const nextUrl = String(value || "").trim();
    if (nextUrl === this.#parquetUrl) return;

    this.#parquetUrl = nextUrl;

    if (nextUrl) {
      this.#loadAndRedrawFromParquetUrl(nextUrl);
    } else {
      this.#rows = [];
      this.#redrawFromData();
    }
  }

  get pointSize() {
    return this.#pointSize;
  }
  set pointSize(value) {
    const parsed = Number(value);
    const nextSize = Number.isFinite(parsed) ? parsed : 4;

    if (nextSize === this.#pointSize) return;

    this.#pointSize = nextSize;
    this.#initializeOrResize(true);
    this.#redrawFromData();
  }

  get clusterId() {
    return this.#clusterIdInput;
  }
  set clusterId(value) {
    const nextClusterId = String(value || "cluster_id").trim() || "cluster_id";
    if (nextClusterId === this.#clusterIdInput) return;

    this.#clusterIdInput = nextClusterId;
    this.#paletteAppliedKey = "";
    this.#legendAppliedKey = "";
    this.#redrawFromData();
  }

  get x() {
    return this.#xOverride;
  }
  set x(value) {
    const next = String(value || "").trim();
    if (next === this.#xOverride) return;

    this.#xOverride = next;
    this.#redrawFromData();
  }

  get y() {
    return this.#yOverride;
  }
  set y(value) {
    const next = String(value || "").trim();
    if (next === this.#yOverride) return;

    this.#yOverride = next;
    this.#redrawFromData();
  }

  get selectedClusterName() {
    return this.#selectedClusterName;
  }
  set selectedClusterName(value) {
    const next = String(value || "").trim();
    if (next === this.#selectedClusterName) return;
    this.#selectedClusterName = next;
  }

  connectedCallback() {
    this.#parquetUrl = readStringAttribute(this, "parquet-url", this.#parquetUrl);

    const xAttribute = readStringAttribute(this, "x", "");
    if (xAttribute) this.#xOverride = xAttribute;

    const yAttribute = readStringAttribute(this, "y", "");
    if (yAttribute) this.#yOverride = yAttribute;

    this.#pointSize = readNumberAttribute(this, "point-size", this.#pointSize);

    const clusterIdAttribute = readStringAttribute(this, "cluster-id", "");
    if (clusterIdAttribute) this.#clusterIdInput = clusterIdAttribute;

    this.#selectedClusterName = readStringAttribute(
      this,
      "selected-cluster-name",
      this.#selectedClusterName
    );

    this.#initializeOrResize(true);

    this.#canvas.addEventListener("mousemove", this.#onCanvasMouseMove);
    this.#canvas.addEventListener("mouseleave", this.#onCanvasMouseLeave);

    if (this.#parquetUrl) this.#loadAndRedrawFromParquetUrl(this.#parquetUrl);
    else this.#redrawFromData();

    this.#resizeObserver = new ResizeObserver(() => {
      this.#initializeOrResize(false);
      if (this.#lastDrawnPoints.length > 0) {
        this.#queueDraw(this.#lastDrawnPoints);
      }
    });
    this.#resizeObserver.observe(this);

    // DEV/DEBUG: allow forcing clusters-changed emission from the console without
    // relying on host bindings. Safe in production (no-op unless used).
    if (typeof window !== "undefined") {
      window.__rmxScatterDebug = window.__rmxScatterDebug || {};
      window.__rmxScatterDebug.pokeClusters = () => {
        try {
          this.#legendAppliedKey = "";
          // Force a redraw so legend recomputes and clusters-changed re-emits.
          this.#redrawFromData();
          console.log("[rmx-scatterplot] pokeClusters done");
        } catch (e) {
          console.error("[rmx-scatterplot] pokeClusters error", e);
        }
      };
    }
  }

  disconnectedCallback() {
    this.#resizeObserver?.disconnect();
    this.#resizeObserver = null;

    this.#canvas.removeEventListener("mousemove", this.#onCanvasMouseMove);
    this.#canvas.removeEventListener("mouseleave", this.#onCanvasMouseLeave);

    this.#hideTooltip();

    try {
      this.#fetchAbortController?.abort();
    } catch {
      // ignore
    }
    this.#fetchAbortController = null;

    try {
      this.#scatterplot?.destroy?.();
    } catch {
      // ignore
    }
    this.#scatterplot = null;
  }

  attributeChangedCallback(name, oldValue, newValue) {
    if (oldValue === newValue) return;

    switch (name) {
      case "parquet-url":
        this.parquetUrl = newValue;
        break;
      case "x":
        this.x = newValue;
        break;
      case "y":
        this.y = newValue;
        break;
      case "point-size":
        this.pointSize = newValue;
        break;
      case "cluster-id":
        this.clusterId = newValue;
        break;
      case "selected-cluster-name":
        this.selectedClusterName = newValue;
        break;
      default:
        break;
    }
  }

  async #loadAndRedrawFromParquetUrl(url) {
    const trimmedUrl = String(url || "").trim();
    if (!trimmedUrl) return;

    try {
      this.#fetchAbortController?.abort();
    } catch {
      // ignore
    }
    this.#fetchAbortController = new AbortController();

    try {
      const loadedRows = await this.#fetchRowsFromParquetUrl(
        trimmedUrl,
        this.#fetchAbortController.signal
      );

      // If the attribute changed while we were loading, drop the result.
      if (trimmedUrl !== this.#parquetUrl) return;

      this.#rows = Array.isArray(loadedRows) ? loadedRows : [];
      this.#inferXYKeys();

      this.#paletteAppliedKey = "";
      this.#legendAppliedKey = "";

      this.#redrawFromData();
    } catch (error) {
      if (error?.name === "AbortError") return;

      console.warn("rmx-scatterplot: failed to load parquet-url", error);
      this.#rows = [];
      this.#redrawFromData();
    }
  }

  async #fetchRowsFromParquetUrl(url, signal) {
    const file = await asyncBufferFromUrl({
      url,
      requestInit: {
        signal,
        credentials: "omit",
        mode: "cors",
        cache: "no-store",
      },
    });

    return parquetReadObjects({ file });
  }

  #inferXYKeys() {
    this.#inferredXKey = null;
    this.#inferredYKey = null;

    const firstRow = this.#rows.length > 0 ? this.#rows[0] : null;
    if (!firstRow) return;

    const xCandidates = ["x", "projection_x", "umap_x", "tsne_x", "pca_x", "x0"];
    const yCandidates = ["y", "projection_y", "umap_y", "tsne_y", "pca_y", "y0"];

    const xKey = pickFirstExistingKey(firstRow, xCandidates);
    const yKey = pickFirstExistingKey(firstRow, yCandidates);

    if (xKey && yKey) {
      this.#inferredXKey = xKey;
      this.#inferredYKey = yKey;
      return;
    }

    const scanCount = Math.min(25, this.#rows.length);
    for (let rowIndex = 0; rowIndex < scanCount; rowIndex++) {
      const row = this.#rows[rowIndex];
      const maybeX = pickFirstExistingKey(row, xCandidates);
      const maybeY = pickFirstExistingKey(row, yCandidates);
      if (maybeX && maybeY) {
        this.#inferredXKey = maybeX;
        this.#inferredYKey = maybeY;
        return;
      }
    }

    this.#inferredXKey = "x";
    this.#inferredYKey = "y";
  }

  #initializeOrResize(forceRecreate) {
    const rect = this.getBoundingClientRect();
    const cssWidth = Math.max(1, rect.width);
    const cssHeight = Math.max(1, rect.height);
    const pixelRatio = window.devicePixelRatio || 1;

    this.#canvas.style.width = "100%";
    this.#canvas.style.height = "100%";

    this.#canvas.width = Math.max(1, Math.floor(cssWidth * pixelRatio));
    this.#canvas.height = Math.max(1, Math.floor(cssHeight * pixelRatio));

    const options = {
      canvas: this.#canvas,
      width: cssWidth,
      height: cssHeight,
      pixelRatio,
      pointSize: Math.max(1, Number.isFinite(this.#pointSize) ? this.#pointSize : 4),
      lassoMinDelay: 0,
    };

    if (!this.#scatterplot || forceRecreate) {
      try {
        this.#scatterplot?.destroy?.();
      } catch {
        // ignore
      }

      this.#scatterplot = createScatterplot(options);
      this.#scatterplot.set({ colorBy: "valueA" });

      this.#wireScatterplotSelectionEvents();
      this.#wireScatterplotHoverEvents();
      return;
    }

    this.#scatterplot.set({
      width: cssWidth,
      height: cssHeight,
      pixelRatio,
    });
  }

  #wireScatterplotSelectionEvents() {
    const scatterplot = this.#scatterplot;
    if (!scatterplot || typeof scatterplot.subscribe !== "function") return;

    scatterplot.subscribe("select", (event) => {
      const rawPoints = event?.points;

      let selectedIndices = [];
      if (rawPoints && typeof rawPoints.length === "number") {
        selectedIndices = Array.from(rawPoints);
      } else if (rawPoints && typeof rawPoints[Symbol.iterator] === "function") {
        selectedIndices = Array.from(rawPoints);
      }

      const selectedRows = selectedIndices
        .map((index) => this.#validRows[index])
        .filter(Boolean);

      // Always emit list selection for state-reset ergonomics.
      this.dispatchEvent(
        new CustomEvent("selected-points", {
          detail: jsonSafe(selectedRows),
          bubbles: true,
          composed: true,
        })
      );

      if (selectedRows.length === 1) {
        this.dispatchEvent(
          new CustomEvent("selected-point", {
            detail: jsonSafe(selectedRows[0]),
            bubbles: true,
            composed: true,
          })
        );
      } else {
        // When it isn't a single selection, still reset the single selection downstream.
        this.dispatchEvent(
          new CustomEvent("selected-point", {
            detail: null,
            bubbles: true,
            composed: true,
          })
        );
      }
    });

    scatterplot.subscribe("deselect", () => {
      this.dispatchEvent(
        new CustomEvent("selected-points", {
          detail: [],
          bubbles: true,
          composed: true,
        })
      );

      this.dispatchEvent(
        new CustomEvent("selected-point", {
          detail: null,
          bubbles: true,
          composed: true,
        })
      );
    });
  }

  #wireScatterplotHoverEvents() {
    const scatterplot = this.#scatterplot;
    if (!scatterplot || typeof scatterplot.subscribe !== "function") return;

    const scheduleTooltipForIndex = (maybeIndex) => {
      const index = Number.isFinite(maybeIndex) ? maybeIndex : null;

      const outOfRange =
        index == null || index < 0 || index >= this.#validRows.length;

      if (outOfRange) {
        this.#hoveredIndex = null;
        this.#hideTooltip();
        return;
      }

      if (this.#hoveredIndex === index) return;
      this.#hoveredIndex = index;

      const now = Date.now();
      const isWarm = now - this.#lastHoverShowAt <= this.#hoverWarmWindowMs;
      const delayMs = isWarm ? this.#hoverWarmDelayMs : this.#hoverColdDelayMs;

      clearTimeout(this.#hoverShowTimer);
      this.#hoverShowTimer = setTimeout(() => {
        this.#showTooltipForIndex(index);
        this.#lastHoverShowAt = Date.now();
      }, delayMs);
    };

    scatterplot.subscribe("pointover", (event) => {
      const index = event?.index ?? event?.point ?? event;
      scheduleTooltipForIndex(index);
    });

    scatterplot.subscribe("pointout", () => {
      this.#hoveredIndex = null;
      this.#hideTooltip();
    });

    scatterplot.subscribe("hover", (event) => {
      const index = event?.index ?? event?.point ?? event;
      if (index == null) return;
      scheduleTooltipForIndex(index);
    });

    scatterplot.subscribe("mousemove", (event) => {
      const index = event?.index ?? event?.point ?? event;
      if (index == null) return;
      scheduleTooltipForIndex(index);
    });
  }

  #positionTooltip(mouseX, mouseY) {
    const padding = 12;
    const rootRect = this.#root.getBoundingClientRect();

    const maxX = rootRect.width;
    const maxY = rootRect.height;

    const tooltip = this.#tooltip;
    const tooltipWidth = tooltip.offsetWidth || 320;
    const tooltipHeight = tooltip.offsetHeight || 140;

    let left = mouseX + 14;
    let top = mouseY + 14;

    if (left + tooltipWidth + padding > maxX) {
      left = Math.max(padding, mouseX - tooltipWidth - 14);
    }

    if (top + tooltipHeight + padding > maxY) {
      top = Math.max(padding, mouseY - tooltipHeight - 14);
    }

    tooltip.style.left = `${left}px`;
    tooltip.style.top = `${top}px`;
    tooltip.style.transform = "translate(0, 0)";
  }

  #showTooltipForIndex(index) {
    const row = this.#validRows[index];
    if (!row) return;

    this.#tooltip.textContent = formatTooltipLines(row, 60);
    this.#tooltip.style.display = "block";
    this.#positionTooltip(this.#mousePosition.x, this.#mousePosition.y);
  }

  #hideTooltip() {
    clearTimeout(this.#hoverShowTimer);
    this.#hoverShowTimer = null;
    this.#tooltip.style.display = "none";
  }

  #redrawFromData() {
    if (!this.#scatterplot) return;

    const rows = Array.isArray(this.#rows) ? this.#rows : [];

    this.#validRows = [];
    this.#clusterToIndices.clear();

    if (!this.#inferredXKey || !this.#inferredYKey) this.#inferXYKeys();

    const firstRow = rows[0] || null;

    const requestedX = String(this.#xOverride || "").trim();
    const requestedY = String(this.#yOverride || "").trim();

    const inferredX = this.#inferredXKey || "x";
    const inferredY = this.#inferredYKey || "y";

    const xKey =
      requestedX &&
        firstRow &&
        Object.prototype.hasOwnProperty.call(firstRow, requestedX)
        ? requestedX
        : inferredX;

    const yKey =
      requestedY &&
        firstRow &&
        Object.prototype.hasOwnProperty.call(firstRow, requestedY)
        ? requestedY
        : inferredY;

    this.#effectiveXKey = xKey;
    this.#effectiveYKey = yKey;

    const requestedClusterKey = this.#clusterIdInput || "cluster_id";
    const clusterKey = chooseBestClusterKey(rows, requestedClusterKey);
    this.#effectiveClusterKey = clusterKey;

    // Unique cluster labels -> palette index mapping
    const clusterValues = rows.map((row) => row?.[clusterKey]);
    const uniqueClusterLabels = Array.from(
      new Set(
        clusterValues
          .filter(isUsableClusterValue)
          .map((value) => String(value))
      )
    );

    if (uniqueClusterLabels.length === 0) uniqueClusterLabels.push("0");

    const clusterLabelToPaletteIndex = new Map();
    uniqueClusterLabels.forEach((label, index) =>
      clusterLabelToPaletteIndex.set(label, index)
    );

    const palette = uniqueClusterLabels.map((_, index) => {
      const hue01 =
        uniqueClusterLabels.length <= 1 ? 0 : index / uniqueClusterLabels.length;
      return rgb01ToHex(hsvToRgb01(hue01, 0.55, 0.95));
    });

    const paletteKey = `cluster:${clusterKey}:k=${uniqueClusterLabels.length}:n=${rows.length}`;
    if (paletteKey !== this.#paletteAppliedKey) {
      this.#scatterplot.set({
        colorBy: "valueA",
        pointColor: palette,
      });
      this.#paletteAppliedKey = paletteKey;
    }

    // Legend payload (name/color/count)
    const clusterCounts = new Map(uniqueClusterLabels.map((label) => [label, 0]));

    const points = [];

    for (const row of rows) {
      const xValue = toFiniteNumber(row?.[xKey]);
      const yValue = toFiniteNumber(row?.[yKey]);
      if (!Number.isFinite(xValue) || !Number.isFinite(yValue)) continue;

      const rawClusterValue = row?.[clusterKey];
      const clusterLabel = isUsableClusterValue(rawClusterValue)
        ? String(rawClusterValue)
        : uniqueClusterLabels[0];

      const paletteIndex = clusterLabelToPaletteIndex.has(clusterLabel)
        ? clusterLabelToPaletteIndex.get(clusterLabel)
        : 0;

      const validRowIndex = this.#validRows.length;
      this.#validRows.push(row);

      if (!this.#clusterToIndices.has(clusterLabel)) {
        this.#clusterToIndices.set(clusterLabel, []);
      }
      this.#clusterToIndices.get(clusterLabel).push(validRowIndex);

      clusterCounts.set(clusterLabel, (clusterCounts.get(clusterLabel) || 0) + 1);

      // regl-scatterplot expects [x, y, valueA] when using colorBy: "valueA"
      points.push([xValue, yValue, paletteIndex]);
    }

    this.#lastDrawnPoints = points;
    this.#queueDraw(points);

    const legendPayload = uniqueClusterLabels.map((name, index) => ({
      name,
      color: palette[index],
      count: clusterCounts.get(name) || 0,
    }));
    if (legendPayload.length) this.#lastNonEmptyLegendPayload = legendPayload;

    const legendKey = `${paletteKey}|legend:${legendPayload.length}|${legendPayload
      .map((entry) => `${entry.name}:${entry.count}`)
      .join(",")}`;

    const debug = this.hasAttribute("debug");

    if (debug || legendKey !== this.#legendAppliedKey) {
      this.#legendAppliedKey = legendKey;

      if (debug) {
        try {
          console.log("[rmx-scatterplot clusters debug]", {
            rows: this.#rows?.length,
            validRows: this.#validRows?.length,
            clusterIdInput: this.#clusterIdInput,
            selectedClusterName: this.#selectedClusterName,
            legendPayloadLen: legendPayload?.length,
            legendPayloadPreview: legendPayload?.slice?.(0, 5),
          });
        } catch (e) {
          console.warn("[rmx-scatterplot clusters debug] failed", e);
        }
      }

      const toSend = (legendPayload.length ? legendPayload : (this.#lastNonEmptyLegendPayload || legendPayload));
      const payload = JSON.parse(JSON.stringify(jsonSafe(toSend)));

      const dispatch = () => {
        this.dispatchEvent(
          new CustomEvent("clusters-changed", {
            detail: payload,
            bubbles: true,
            composed: true,
          })
        );
      };

      requestAnimationFrame(dispatch);
      requestAnimationFrame(() => requestAnimationFrame(dispatch));
    }
  }

  #queueDraw(points) {
    this.#pendingPoints = points;
    if (this.#drawScheduled) return;

    this.#drawScheduled = true;

    queueMicrotask(async () => {
      this.#drawScheduled = false;
      if (this.#drawInFlight) return;

      this.#drawInFlight = true;
      try {
        const pointsToDraw = this.#pendingPoints || [];
        this.#pendingPoints = null;
        await this.#scatterplot.draw(pointsToDraw);
      } finally {
        this.#drawInFlight = false;
      }
    });
  }
}

customElements.define("rmx-scatterplot", RmxScatterplot);
