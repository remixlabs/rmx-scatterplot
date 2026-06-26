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
      "point-size-selected",
      "point-opacity",
      "cluster-id",
      "selected-cluster-name",
      "show-cluster-labels",
      "cluster-label-key",
      "background-color",
      "lasso-color",
      "tooltip-key",
    ];
  }

  // DOM
  #root;
  #canvas;
  #tooltip;

  #labelsLayer;

  // Cluster label overlay
  #showClusterLabels;
  #clusterLabelKeyOverride;
  #clusterIdToDisplayLabel;
  #clusterLabelToId;
  #labelElementsByCluster;
  #labelLayoutRaf;
  #warnedMissingScreenPos;
  #needsInitialFit;

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
  #pointSizeSelected;
  #pointOpacity;
  #backgroundColor;
  #lassoColor;
  #tooltipKey;
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

    this.#labelsLayer = document.createElement("div");
    this.#labelsLayer.id = "labels";

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

      #labels {
        position:absolute;
        inset:0;
        pointer-events:none;
        z-index:5;
      }
      .cluster-label {
        position:absolute;
        transform: translate(-50%, -50%);
        padding: 2px 6px;
        border-radius: 999px;
        border: 1px solid rgba(255,255,255,0.18);
        background: rgba(0,0,0,0.55);
        backdrop-filter: blur(6px);
        color: rgba(255,255,255,0.92);
        font-family: ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
        font-size: 12px;
        line-height: 1.2;
        white-space: nowrap;
        max-width: 260px;
        overflow: hidden;
        text-overflow: ellipsis;
        box-shadow: 0 10px 24px rgba(0,0,0,0.25);
      }
    `;

    this.shadowRoot.append(style, this.#root);
    this.#root.append(this.#canvas, this.#labelsLayer, this.#tooltip);

    this.#scatterplot = null;
    this.#resizeObserver = null;

    this.#rows = [];
    this.#validRows = [];
    this.#lastDrawnPoints = [];

    this.#parquetUrl = "";
    this.#fetchAbortController = null;

    this.#pointSize = 4;
    this.#pointSizeSelected = 6;
    this.#pointOpacity = 1;
    this.#backgroundColor = "#000000";
    this.#lassoColor = "#ffffff";
    this.#tooltipKey = "";
    this.#clusterIdInput = "cluster_id";
    this.#xOverride = "projection_x";
    this.#yOverride = "projection_y";
    this.#selectedClusterName = "";

    this.#showClusterLabels = true;
    this.#clusterLabelKeyOverride = "";
    this.#clusterIdToDisplayLabel = new Map();
    this.#labelElementsByCluster = new Map();
    this.#labelLayoutRaf = 0;
    this.#warnedMissingScreenPos = false;
    this.#needsInitialFit = true;

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
    const selected = this.#resolveClusterSelectionValue(this.#selectedClusterName);
    if (!selected || !this.#scatterplot) return;

    const indices = this.#clusterToIndices.get(selected) || [];
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
    if (this.#scatterplot) {
      this.#scatterplot.set({ pointSize: nextSize });
      if (this.#lastDrawnPoints.length > 0) this.#queueDraw(this.#lastDrawnPoints);
    } else {
      this.#initializeOrResize(true);
      this.#redrawFromData();
    }
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
    this.#pointSizeSelected = readNumberAttribute(this, "point-size-selected", this.#pointSizeSelected);
    this.#pointOpacity = readNumberAttribute(this, "point-opacity", this.#pointOpacity);

    const bgAttr = readStringAttribute(this, "background-color", "");
    if (bgAttr) this.#backgroundColor = bgAttr;

    const lassoAttr = readStringAttribute(this, "lasso-color", "");
    if (lassoAttr) this.#lassoColor = lassoAttr;

    const tooltipAttr = readStringAttribute(this, "tooltip-key", "");
    if (tooltipAttr) this.#tooltipKey = tooltipAttr;

    const clusterIdAttribute = readStringAttribute(this, "cluster-id", "");
    if (clusterIdAttribute) this.#clusterIdInput = clusterIdAttribute;

    this.#selectedClusterName = readStringAttribute(
      this,
      "selected-cluster-name",
      this.#selectedClusterName
    );

    // Cluster label overlay controls (optional)
    const showLabelsAttr = readStringAttribute(this, "show-cluster-labels", "");
    if (showLabelsAttr) {
      this.#showClusterLabels = showLabelsAttr !== "false";
    }

    this.#clusterLabelKeyOverride = readStringAttribute(this, "cluster-label-key", this.#clusterLabelKeyOverride);

    this.#initializeOrResize(true);

    this.#canvas.addEventListener("mousemove", this.#onCanvasMouseMove);
    this.#canvas.addEventListener("mouseleave", this.#onCanvasMouseLeave);

    // Keep overlay labels aligned during zoom/pan.
    this.#canvas.addEventListener("wheel", () => this.#scheduleClusterLabelLayout(), { passive: true });
    this.#canvas.addEventListener("pointerdown", () => this.#scheduleClusterLabelLayout());
    this.#canvas.addEventListener("pointermove", () => this.#scheduleClusterLabelLayout());
    this.#canvas.addEventListener("pointerup", () => this.#scheduleClusterLabelLayout());

    if (this.#parquetUrl) this.#loadAndRedrawFromParquetUrl(this.#parquetUrl);
    else this.#redrawFromData();

    this.#resizeObserver = new ResizeObserver(() => {
      this.#initializeOrResize(false);
      if (this.#lastDrawnPoints.length > 0) {
        this.#queueDraw(this.#lastDrawnPoints);
        this.#scheduleClusterLabelLayout(false);
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
    // Reset draw-state flags so connectedCallback can draw again after reconnect.
    // If a draw() promise was in-flight when the scatterplot was destroyed it will
    // never settle, keeping #drawInFlight=true and blocking all future draws.
    this.#drawInFlight = false;
    this.#drawScheduled = false;
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
      case "show-cluster-labels":
        this.#showClusterLabels = String(newValue || "").trim() !== "false";
        this.#scheduleClusterLabelLayout(true);
        break;
      case "cluster-label-key":
        this.#clusterLabelKeyOverride = String(newValue || "").trim();
        this.#scheduleClusterLabelLayout(true);
        break;
      case "point-size-selected": {
        const v = Number(newValue);
        if (Number.isFinite(v) && v > 0) {
          this.#pointSizeSelected = v;
          this.#scatterplot?.set({ pointSizeSelected: v });
        }
        break;
      }
      case "point-opacity": {
        const v = Number(newValue);
        if (Number.isFinite(v)) {
          this.#pointOpacity = Math.min(1, Math.max(0, v));
          // Only override AUTO when not at full opacity.
          if (this.#pointOpacity !== 1) {
            this.#scatterplot?.set({ opacity: this.#pointOpacity });
          }
        }
        break;
      }
      case "background-color":
        if (newValue) {
          this.#backgroundColor = newValue;
          this.#scatterplot?.set({ backgroundColor: newValue });
        }
        break;
      case "lasso-color":
        if (newValue) {
          this.#lassoColor = newValue;
          this.#scatterplot?.set({ lassoColor: newValue });
        }
        break;
      case "tooltip-key":
        this.#tooltipKey = String(newValue || "").trim();
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
    // Don't pass signal to asyncBufferFromUrl — hyparquet forwards it to the
    // HEAD request for byte-length, which gets aborted by connectedCallback's
    // abort-and-restart pattern, killing the entire load. Check manually after.
    const file = await asyncBufferFromUrl({
      url,
      requestInit: {
        credentials: "omit",
        mode: "cors",
        cache: "no-store",
      },
    });

    if (signal?.aborted) throw new DOMException("Aborted", "AbortError");
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

    const key = this.#tooltipKey;
    if (key && Object.prototype.hasOwnProperty.call(row, key)) {
      this.#tooltip.textContent = String(row[key] ?? "");
    } else {
      this.#tooltip.textContent = formatTooltipLines(row, 60);
    }
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
    this.#needsInitialFit = true;
    this.#queueDraw(points);
    this.#scheduleClusterLabelLayout(true);

    
    // Prefer human-readable cluster labels (e.g. "Billing refunds") for display,
    // but keep the original cluster id around for stable selection.
    const clusterIdToLabel = new Map();
    const clusterLabelToId = new Map();

    // Try to find a label key in the row data (case-insensitive).
    const labelKeyCandidates = [
      this.#clusterLabelKeyOverride,
      "cluster_label",
      "clusterLabel",
      "label",
      "topic_label",
      "topicLabel",
    ].filter(Boolean);

    const firstRowForLabels = this.#validRows[0] || this.#rows[0] || null;
    let resolvedLabelKey = null;

    if (firstRowForLabels) {
      const lowerToActual = new Map(
        Object.keys(firstRowForLabels).map((k) => [k.toLowerCase(), k])
      );

      for (const candidate of labelKeyCandidates) {
        const actual = lowerToActual.get(String(candidate).toLowerCase());
        if (actual) {
          resolvedLabelKey = actual;
          break;
        }
      }
    }

    // Build clusterId -> label using the first non-empty label we can find in each cluster.
    for (const clusterId of uniqueClusterLabels) {
      const ids = this.#clusterToIndices.get(clusterId) || [];
      let displayLabel = null;

      if (resolvedLabelKey && ids.length) {
        for (const idx of ids) {
          const row = this.#validRows[idx];
          if (!row) continue;

          const raw = row[resolvedLabelKey];
          if (isUsableClusterValue(raw)) {
            displayLabel = String(raw).trim();
            if (displayLabel) break;
          }
        }
      }

      const finalLabel = displayLabel || String(clusterId);
      clusterIdToLabel.set(String(clusterId), finalLabel);

      // Only map label->id when it's unambiguous; keep the first one.
      if (!clusterLabelToId.has(finalLabel)) {
        clusterLabelToId.set(finalLabel, String(clusterId));
      }
    }

    // Stash for selection resolution (label -> id) and for overlay labels.
    this.#clusterIdToDisplayLabel.clear();
    for (const [id, label] of clusterIdToLabel.entries()) {
      this.#clusterIdToDisplayLabel.set(id, label);
    }
    this.#clusterLabelToId = clusterLabelToId;

    const legendPayload = uniqueClusterLabels.map((clusterId, index) => ({
      id: String(clusterId),
      name: clusterIdToLabel.get(String(clusterId)) || String(clusterId), // display name
      color: palette[index],
      count: clusterCounts.get(clusterId) || 0,
    }));
// Cache simple cluster display labels (fallback when cluster_label column is missing).
    this.#clusterIdToDisplayLabel.clear();
    for (const entry of legendPayload) {
      this.#clusterIdToDisplayLabel.set(String(entry.name), String(entry.name));
    }

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

  #resolveClusterSelectionValue(selectionValue) {
    const raw = String(selectionValue || "").trim();
    if (!raw) return "";

    // If the selection matches a cluster id, prefer that.
    if (this.#clusterToIndices?.has(raw)) return raw;

    // Otherwise, allow passing a display label from an external legend.
    const byLabel = this.#clusterLabelToId?.get(raw);
    return byLabel || raw;
  }


  #scheduleClusterLabelLayout(forceRebuild = false) {
    if (!this.#showClusterLabels) {
      this.#labelsLayer.innerHTML = "";
      this.#labelElementsByCluster.clear();
      return;
    }

    if (forceRebuild) {
      this.#labelElementsByCluster.clear();
      this.#labelsLayer.innerHTML = "";
    }

    if (this.#labelLayoutRaf) return;

    this.#labelLayoutRaf = requestAnimationFrame(() => {
      this.#labelLayoutRaf = 0;
      try {
        this.#layoutClusterLabels();
      } catch (e) {
        // Never let overlay issues break the scatterplot render path.
        console.warn("[rmx-scatterplot] cluster label overlay failed", e);
      }
    });
  }

  #layoutClusterLabels() {
    if (!this.#showClusterLabels) return;
    if (!this.#scatterplot) return;

    const getScreenPosition = this.#scatterplot.getScreenPosition;
    if (typeof getScreenPosition !== "function") {
      if (!this.#warnedMissingScreenPos) {
        this.#warnedMissingScreenPos = true;
        console.warn(
          "[rmx-scatterplot] regl-scatterplot.getScreenPosition() not available; cannot render in-chart cluster labels."
        );
      }
      return;
    }

    // Choose a label key in the row data (case-insensitive).
    const firstRow = this.#validRows[0] || this.#rows[0] || null;
    const labelKeyCandidates = [
      this.#clusterLabelKeyOverride,
      "cluster_label",
      "clusterLabel",
      "label",
      "topic_label",
      "topicLabel",
    ].filter(Boolean);

    let resolvedLabelKey = null;
    if (firstRow) {
      const rowKeys = Object.keys(firstRow);
      const lowerToActual = new Map(rowKeys.map((k) => [k.toLowerCase(), k]));
      for (const candidate of labelKeyCandidates) {
        const actual = lowerToActual.get(String(candidate).toLowerCase());
        if (actual) {
          resolvedLabelKey = actual;
          break;
        }
      }
    }

    // Build a representative index per cluster (closest-to-centroid in 2D).
    const clusters = Array.from(this.#clusterToIndices.entries());
    if (clusters.length === 0) return;

    // Precompute xy for each point in drawn space (aligned to validRows)
    // We can reconstruct x/y from lastDrawnPoints, which is aligned with validRows indices.
    const points = this.#lastDrawnPoints || [];
    if (points.length === 0) return;

    for (const [clusterId, indices] of clusters) {
      if (!indices || indices.length === 0) continue;

      // Centroid
      let sumX = 0;
      let sumY = 0;
      let count = 0;
      for (const idx of indices) {
        const p = points[idx];
        if (!p) continue;
        sumX += p[0];
        sumY += p[1];
        count++;
      }
      if (count === 0) continue;

      const cx = sumX / count;
      const cy = sumY / count;

      // Closest index to centroid
      let bestIdx = indices[0];
      let bestDist = Infinity;
      for (const idx of indices) {
        const p = points[idx];
        if (!p) continue;
        const dx = p[0] - cx;
        const dy = p[1] - cy;
        const d = dx * dx + dy * dy;
        if (d < bestDist) {
          bestDist = d;
          bestIdx = idx;
        }
      }

      const row = this.#validRows[bestIdx];
      const clusterLabelFromRow =
        resolvedLabelKey && row && isUsableClusterValue(row[resolvedLabelKey])
          ? String(row[resolvedLabelKey])
          : null;

      const displayLabel =
        clusterLabelFromRow ||
        this.#clusterIdToDisplayLabel.get(clusterId) ||
        String(clusterId);

      const rawScreen = getScreenPosition(bestIdx);

      // regl-scatterplot versions differ:
      // - some return { x: px, y: px }
      // - some return [x, y]
      // - some return normalized device coords in [-1..1]
      let sx = null;
      let sy = null;

      if (Array.isArray(rawScreen) && rawScreen.length >= 2) {
        sx = rawScreen[0];
        sy = rawScreen[1];
      } else if (rawScreen && typeof rawScreen === "object") {
        sx = rawScreen.x;
        sy = rawScreen.y;
      }

      if (!Number.isFinite(sx) || !Number.isFinite(sy)) continue;

      const canvasW = this.#canvas?.clientWidth || 0;
      const canvasH = this.#canvas?.clientHeight || 0;

      // If coords look normalized (roughly in [-1..1]), convert to px.
      if (canvasW > 0 && canvasH > 0 && sx >= -1.05 && sx <= 1.05 && sy >= -1.05 && sy <= 1.05) {
        // NDC: (-1,-1) bottom-left, (1,1) top-right
        sx = (sx * 0.5 + 0.5) * canvasW;
        sy = (1 - (sy * 0.5 + 0.5)) * canvasH;
      }

      let el = this.#labelElementsByCluster.get(clusterId);
      if (!el) {
        el = document.createElement("div");
        el.className = "cluster-label";
        this.#labelsLayer.appendChild(el);
        this.#labelElementsByCluster.set(clusterId, el);
      }

      el.textContent = displayLabel;
      el.style.left = `${sx}px`;
      el.style.top = `${sy}px`;
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
        // Skip empty draws — draw([]) can hang if the scatterplot isn't fully
        // initialized (e.g. 1×1px canvas before connectedCallback), which locks
        // #drawInFlight=true and blocks all future draws.
        if (pointsToDraw.length === 0) return;
        await this.#scatterplot.draw(pointsToDraw);

        // On first render after new data arrives, fit the camera to the full dataset.
        // Without this, the initial view can land "inside" empty space and require a manual zoom-out.
        if (this.#needsInitialFit && typeof this.#scatterplot.fitToBounds === "function") {
          this.#needsInitialFit = false;
          requestAnimationFrame(() => {
            try {
              this.#scatterplot?.fitToBounds?.();
            } catch (e) {
              console.warn("[rmx-scatterplot] fitToBounds failed", e);
            }
          });
        }

        // Labels rely on screen-space buffers; update after a draw completes.
        this.#scheduleClusterLabelLayout(false);
      } finally {
        this.#drawInFlight = false;
      }
    });
  }
}

customElements.define("rmx-scatterplot", RmxScatterplot);
