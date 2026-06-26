"""
Clustering pipeline: JSON → embeddings → UMAP → HDBSCAN → Parquet
Drop-in replacement for the Snowflake notebook, runs fully locally.
"""

import argparse
import json
import sys
import numpy as np
import pandas as pd

# ================================
# USER CONFIG
# ================================

CONFIG = {
    # ---- Source ----
    "json_path": "data.json",           # path to input JSON file
    # JSON can be an array of objects, or a dict with a key containing the array.
    # If a dict, set json_records_key to the key name; otherwise leave None.
    "json_records_key": None,

    # ---- Columns ----
    "id_col": "id",                     # column to use as record ID
    "text_cols": ["text"],              # column(s) to concatenate for embedding
    "passthrough_cols": [],             # extra columns to carry through to the output

    # ---- Embeddings ----
    # Uses sentence-transformers (fully local, no API key needed).
    # Good lightweight default: "all-MiniLM-L6-v2"
    # Higher quality: "all-mpnet-base-v2"
    "embed_model": "all-MiniLM-L6-v2",

    # ---- 2D Projection ----
    "enable_projection": True,
    "projection_method": "umap",        # "umap" or "pca"
    "random_state": 42,
    "umap_n_neighbors": 15,
    "umap_min_dist": 0.0,
    "umap_metric": "cosine",

    # ---- Clustering ----
    "enable_clustering": True,
    "clustering_method": "hdbscan",     # "hdbscan" or "kmeans"
    "clustering_space": "umap",         # "umap" or "embedding"
    "hdbscan_min_cluster_size": 40,
    "hdbscan_min_samples": 5,
    "hdbscan_metric": "euclidean",
    "kmeans_k": 12,

    # ---- Cluster Labels ----
    "enable_cluster_labels": True,
    "cluster_col": "cluster_id",
    "cluster_label_source_columns": [],  # defaults to text_cols if empty
    "emit_cluster_keywords": True,
    "cluster_label_top_terms": 8,
    "cluster_label_ngram_range": (1, 2),
    "cluster_label_max_chars": 60,
    "cluster_label_max_features": 8000,

    # ---- Output ----
    "parquet_filename": "embeddings_full.parquet",
    "atlas_parquet_filename": "atlas_clusters.parquet",
}


# ================================
# PIPELINE
# ================================

def load_json(cfg: dict) -> pd.DataFrame:
    path = cfg["json_path"]
    print(f"Loading JSON: {path}")
    with open(path) as f:
        raw = json.load(f)

    key = cfg.get("json_records_key")
    if isinstance(raw, dict):
        if key:
            raw = raw[key]
        else:
            # Try common wrapper keys
            for k in ("data", "records", "results", "items"):
                if k in raw:
                    print(f"  Auto-detected records key: '{k}'")
                    raw = raw[k]
                    break
            else:
                raise ValueError(
                    "JSON is a dict but json_records_key is not set. "
                    "Set CONFIG['json_records_key'] to the key containing the records array."
                )

    df = pd.DataFrame(raw)
    df.columns = [c.strip().lower() for c in df.columns]
    print(f"  Rows: {len(df)}, Columns: {list(df.columns)}")
    return df


def build_record_text(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    id_col = cfg["id_col"].lower()
    text_cols = [c.lower() for c in cfg["text_cols"]]
    passthrough = [c.lower() for c in cfg.get("passthrough_cols", [])]

    missing = [c for c in [id_col] + text_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Columns not found in data: {missing}. Available: {list(df.columns)}")

    keep = list(dict.fromkeys([id_col] + text_cols + [c for c in passthrough if c in df.columns]))
    df = df[keep].copy()

    df["record_id"] = df[id_col].astype(str)
    df["record_text"] = (
        df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)
        .str.replace(r"\s+", " ", regex=True).str.strip()
    )
    return df


def embed(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    model_name = cfg["embed_model"]
    print(f"Embedding {len(df)} rows with '{model_name}' ...")

    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("Installing sentence-transformers ...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "sentence-transformers"])
        from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    vecs = model.encode(df["record_text"].tolist(), show_progress_bar=True, batch_size=64)
    df["embedding"] = list(vecs.astype(np.float32))
    print(f"  Embedding dim: {vecs.shape[1]}")
    return df


def project(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if not cfg.get("enable_projection", True):
        print("Projection disabled.")
        return df

    method = cfg.get("projection_method", "umap").lower()
    rs = int(cfg.get("random_state", 42))
    mat = np.vstack(df["embedding"].values).astype(np.float32)

    print(f"Projecting {len(df)} rows to 2D via {method.upper()} ...")

    if method == "umap":
        try:
            import umap as umap_lib
        except ImportError:
            print("Installing umap-learn ...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "umap-learn"])
            import umap as umap_lib

        reducer = umap_lib.UMAP(
            n_neighbors=int(cfg.get("umap_n_neighbors", 15)),
            min_dist=float(cfg.get("umap_min_dist", 0.1)),
            metric=str(cfg.get("umap_metric", "cosine")),
            random_state=rs,
        )
        xy = reducer.fit_transform(mat)

    elif method == "pca":
        from sklearn.decomposition import PCA
        xy = PCA(n_components=2, random_state=rs).fit_transform(mat)

    else:
        raise ValueError(f"Unknown projection_method: {method!r}")

    df["projection_x"] = xy[:, 0].astype(float)
    df["projection_y"] = xy[:, 1].astype(float)
    print("  Done.")
    return df


def cluster(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if not cfg.get("enable_clustering", True):
        print("Clustering disabled.")
        return df

    method = cfg.get("clustering_method", "hdbscan").lower()
    space = cfg.get("clustering_space", "umap").lower()

    if space == "umap":
        if "projection_x" not in df.columns:
            raise RuntimeError("clustering_space='umap' requires projection first.")
        feats = df[["projection_x", "projection_y"]].to_numpy(dtype=np.float32)
    else:
        feats = np.vstack(df["embedding"].values).astype(np.float32)

    print(f"Clustering via {method.upper()} in {space.upper()} space ...")

    if method == "hdbscan":
        try:
            import hdbscan as hdbscan_lib
        except ImportError:
            print("Installing hdbscan ...")
            import subprocess
            subprocess.check_call([sys.executable, "-m", "pip", "install", "hdbscan"])
            import hdbscan as hdbscan_lib

        clusterer = hdbscan_lib.HDBSCAN(
            min_cluster_size=int(cfg.get("hdbscan_min_cluster_size", 5)),
            min_samples=int(cfg.get("hdbscan_min_samples", 3)),
            metric=str(cfg.get("hdbscan_metric", "euclidean")),
        )
        labels = clusterer.fit_predict(feats)

    elif method == "kmeans":
        from sklearn.cluster import KMeans
        k = int(cfg.get("kmeans_k", 12))
        labels = KMeans(n_clusters=k, random_state=int(cfg.get("random_state", 42)), n_init="auto").fit_predict(feats)

    else:
        raise ValueError(f"Unknown clustering_method: {method!r}")

    df["cluster_id"] = labels.astype(int)
    n_clusters = len(set(labels)) - (1 if -1 in set(labels) else 0)
    noise = int((labels == -1).sum()) if -1 in set(labels) else 0
    print(f"  clusters={n_clusters}, noise={noise}")
    return df


def label_clusters(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if not cfg.get("enable_cluster_labels", True):
        return df

    cluster_col = str(cfg.get("cluster_col", "cluster_id"))
    if cluster_col not in df.columns:
        print(f"Cluster column '{cluster_col}' not found; skipping labels.")
        return df

    label_src = cfg.get("cluster_label_source_columns") or cfg.get("text_cols", [])
    label_src = [c.lower() for c in label_src]
    label_src = [c for c in label_src if c in df.columns]
    if not label_src:
        print("No label source columns found; skipping cluster labels.")
        return df

    print(f"Generating cluster labels from: {label_src} ...")

    per_row = (
        df[label_src].fillna("").astype(str).agg(" ".join, axis=1)
        .str.replace(r"\s+", " ", regex=True).str.strip()
    )

    tmp = df[[cluster_col]].copy()
    tmp["__text__"] = per_row
    cluster_docs = (
        tmp.groupby(cluster_col)["__text__"]
        .apply(lambda s: " ".join([x for x in s.tolist() if x]))
        .reset_index(name="doc")
    )

    from sklearn.feature_extraction.text import TfidfVectorizer
    top_terms = int(cfg.get("cluster_label_top_terms", 8))
    ngram_range = tuple(cfg.get("cluster_label_ngram_range", (1, 2)))
    max_chars = int(cfg.get("cluster_label_max_chars", 60))

    vectorizer = TfidfVectorizer(
        lowercase=True,
        stop_words="english",
        ngram_range=ngram_range,
        max_features=int(cfg.get("cluster_label_max_features", 8000)),
    )
    X = vectorizer.fit_transform(cluster_docs["doc"].fillna(""))
    terms = np.array(vectorizer.get_feature_names_out())

    labels_out, keywords_out = [], []
    for row_idx, cid in enumerate(cluster_docs[cluster_col].tolist()):
        if str(cid) == "-1":
            labels_out.append("Other / Noise")
            keywords_out.append(["Other / Noise"])
            continue
        row = X.getrow(row_idx)
        if row.nnz == 0:
            labels_out.append(f"Cluster {cid}")
            keywords_out.append([f"Cluster {cid}"])
            continue
        ranked = row.indices[np.argsort(row.data)[::-1]]
        top_phrases = [terms[i] for i in ranked[:top_terms]]
        best = top_phrases[0].replace("_", " ").strip()[:max_chars].strip()
        best = (best[:1].upper() + best[1:]) if best else f"Cluster {cid}"
        labels_out.append(best)
        keywords_out.append(top_phrases)

    label_df = cluster_docs[[cluster_col]].copy()
    label_df["cluster_label"] = labels_out
    if cfg.get("emit_cluster_keywords", True):
        label_df["cluster_keywords"] = keywords_out

    df = df.merge(label_df, on=cluster_col, how="left")
    print(f"  Labeled {label_df.shape[0]} clusters.")
    return df


def write_parquet(df: pd.DataFrame, cfg: dict):
    full_path = cfg.get("parquet_filename", "embeddings_full.parquet")
    atlas_path = cfg.get("atlas_parquet_filename", "atlas_clusters.parquet")

    df.to_parquet(full_path, index=False)
    print(f"Wrote full parquet: {full_path} ({len(df)} rows)")

    # Atlas file: drop embedding column, normalize projections
    required = ["record_id", "record_text", "projection_x", "projection_y"]
    for c in required:
        if c not in df.columns:
            raise ValueError(f"Missing required Atlas column: {c}")

    passthrough = [c.lower() for c in cfg.get("passthrough_cols", [])]
    atlas_cols = required.copy()
    for c in passthrough:
        if c in df.columns and c not in atlas_cols:
            atlas_cols.append(c)
    for c in ["cluster_id", "cluster_label"]:
        if c in df.columns and c not in atlas_cols:
            atlas_cols.append(c)

    atlas = df[atlas_cols].copy()
    atlas = atlas.rename(columns={"record_id": "id", "record_text": "text"})
    atlas["id"] = atlas["id"].astype(str)
    atlas["text"] = atlas["text"].astype(str)
    if "cluster_id" in atlas.columns:
        atlas["cluster_id"] = atlas["cluster_id"].astype(str)

    for col in ["projection_x", "projection_y"]:
        v = atlas[col].astype(float).to_numpy()
        v -= np.mean(v)
        s = np.std(v)
        if s > 0:
            v /= s
        atlas[col] = v.astype(np.float32)

    bad = (
        atlas[["projection_x", "projection_y"]].isna().any(axis=1)
        | ~np.isfinite(atlas["projection_x"].to_numpy())
        | ~np.isfinite(atlas["projection_y"].to_numpy())
    )
    if bad.sum():
        print(f"  Dropping {bad.sum()} rows with invalid projections.")
        atlas = atlas.loc[~bad].reset_index(drop=True)

    atlas.to_parquet(atlas_path, index=False)
    print(f"Wrote atlas parquet: {atlas_path} ({len(atlas)} rows)")
    return atlas_path


def run(cfg: dict):
    df = load_json(cfg)
    df = build_record_text(df, cfg)
    df = embed(df, cfg)
    df = project(df, cfg)
    df = cluster(df, cfg)
    df = label_clusters(df, cfg)
    atlas_path = write_parquet(df, cfg)
    print(f"\nDone. Atlas file ready: {atlas_path}")
    return df


# ================================
# CLI
# ================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JSON → UMAP → HDBSCAN → Parquet")
    parser.add_argument("json_path", nargs="?", help="Path to input JSON file (overrides CONFIG)")
    parser.add_argument("--id-col", help="ID column name")
    parser.add_argument("--text-cols", nargs="+", help="Text column name(s)")
    parser.add_argument("--output", help="Atlas parquet output filename")
    parser.add_argument("--method", choices=["umap", "pca"], help="Projection method")
    parser.add_argument("--cluster", choices=["hdbscan", "kmeans"], help="Clustering method")
    args = parser.parse_args()

    if args.json_path:
        CONFIG["json_path"] = args.json_path
    if args.id_col:
        CONFIG["id_col"] = args.id_col
    if args.text_cols:
        CONFIG["text_cols"] = args.text_cols
    if args.output:
        CONFIG["atlas_parquet_filename"] = args.output
    if args.method:
        CONFIG["projection_method"] = args.method
    if args.cluster:
        CONFIG["clustering_method"] = args.cluster

    run(CONFIG)
