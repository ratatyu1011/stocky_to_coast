# stocky_to_coast.py
import argparse, sys, hashlib, json, logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import csv
import yaml
import pandas as pd
import numpy as np
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check

IN_COLS = ["SKU", "Qty Ordered", "Cost (base)", "Total Cost (base)"]
OUT_COLS = ["Item Id", "Qty Ordered", "Unit Price", "Extended Price"]
STOCKY_ALIASES = {
    "SKU": ["SKU", "Sku", "Item Id", "ItemID"],
    "Qty Ordered": ["Qty Ordered", "Quantity Ordered", "Qty"],
    "Cost (base)": ["Cost (base)", "Unit Cost (base)", "Unit Cost", "Cost"],
    "Total Cost (base)": ["Total Cost (base)", "Extended Price", "Total"],
}

DEFAULT_VENDOR_CFG = {
    "name": "default",
    "output": {
        "columns": ["Item Id", "Qty Ordered", "Unit Price", "Extended Price"],
        "delimiter": ",",
        "decimal_places": 2,
        "quoting": "all"
    }
}

QUOTING_MAP = {
    "all": csv.QUOTE_ALL,
    "minimal": csv.QUOTE_MINIMAL,
    "nonnumeric": csv.QUOTE_NONNUMERIC,
    "none": csv.QUOTE_NONE,
}

def _deep_update(dst: dict, src: dict) -> dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst

def load_vendor_cfg(vendor: str | None, vendor_config_path: str | None):
    cfg = dict(DEFAULT_VENDOR_CFG)  # shallow copy
    data = {}
    if vendor_config_path:
        with open(vendor_config_path, "r") as f:
            data = yaml.safe_load(f) or {}
    elif vendor:
        path = Path("vendor_configs") / f"{vendor}.yml"
        with open(path, "r") as f:
            data = yaml.safe_load(f) or {}
    return _deep_update(cfg, data)

def map_to_required(df: pd.DataFrame) -> pd.DataFrame:

    cols = {}
    for canonical, aliases in STOCKY_ALIASES.items():
        for a in aliases:
            if a in df.columns or a.lower() in map(str.lower, df.columns):
                real = next((c for c in df.columns if c.lower() == a.lower()), None)
                if real:
                    cols[canonical] = df[real]
                    break
        if canonical not in cols:
            raise KeyError(f"Missing required column: {canonical}")
    out = pd.DataFrame(cols)
    out["Qty Ordered"] = pd.to_numeric(out["Qty Ordered"], errors="raise").astype(int)
    for c in ["Cost (base)", "Total Cost (base)"]:
        out[c] = pd.to_numeric(out[c], errors="raise")
    return out

def setup_logging(run_dir: Path):
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("stocky2coast")
    logger.setLevel(logging.INFO)
    # Avoid duplicate handlers when called via tests repeatedly
    if not logger.handlers:
        handler = RotatingFileHandler(run_dir / "stocky2coast.log", maxBytes=200_000, backupCount=3)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger

def schema():
    return DataFrameSchema(
        {
            "SKU": Column(str, nullable=False),
            "Qty Ordered": Column(int, Check.ge(0), nullable=False),
            "Cost (base)": Column(float, Check.ge(0.0), nullable=False),
            "Total Cost (base)": Column(float, Check.ge(0.0), nullable=False),
        },
        checks=Check(
            lambda df: ((df["Qty Ordered"] * df["Cost (base)"]) - df["Total Cost (base)"]).abs() <= 0.01,
            error="Row total mismatch: |Qty*Cost - Total| > 0.01",
        ),
        strict=True,
        coerce=True
    )

def dedupe_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    g = df.groupby("SKU", as_index=False).agg({
        "Qty Ordered": "sum",
        "Cost (base)": "first"  # assuming unit cost is consistent within a PO per SKU
    })
    g["Total Cost (base)"] = (g["Qty Ordered"] * g["Cost (base)"]).round(2)
    for c in ["Cost (base)", "Total Cost (base)"]:
        g[c] = g[c].round(2)
    return g

def to_coast(df: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "Item Id": df["SKU"],
        "Qty Ordered": df["Qty Ordered"],
        "Unit Price": df["Cost (base)"].round(2),
        "Extended Price": (df["Qty Ordered"] * df["Cost (base)"]).round(2),
    })
    return out[OUT_COLS]

def hash_output(df: pd.DataFrame) -> str:
    b = df.to_csv(index=False).encode("utf-8")
    return hashlib.md5(b).hexdigest()[:8]

def variance_flags(df: pd.DataFrame, price_hist_path: Path, threshold=0.20):
    if not price_hist_path or not price_hist_path.exists():
        return []
    hist = pd.read_csv(price_hist_path)  # columns: SKU,LastCost
    if "LastCost" not in hist.columns or "SKU" not in hist.columns:
        return []
    hist = hist[hist["LastCost"] > 0]
    merged = df.merge(hist, on="SKU", how="left")
    merged["pct_change"] = (merged["Cost (base)"] - merged["LastCost"]) / merged["LastCost"]
    flg = merged[(merged["LastCost"].notna()) & (merged["pct_change"].abs() > threshold)]
    return flg[["SKU", "LastCost", "Cost (base)", "pct_change"]].to_dict(orient="records")

def main():
    ap = argparse.ArgumentParser(description="Convert Stocky PO CSV to Coast cart CSV with validation.")
    ap.add_argument("--po", required=True, help="PO number, e.g. 1848")
    ap.add_argument("--input", required=True, help="Path to Stocky CSV (po_XXXX.csv)")
    ap.add_argument("--outdir", default="runs", help="Output directory for artifacts")
    ap.add_argument("--price-history", default="", help="Optional price_history.csv with columns SKU,LastCost")
    grp = ap.add_mutually_exclusive_group(required=False)
    grp.add_argument("--vendor", choices=["coast", "erikson_music", "erikson_audio"],
                    help="Built-in vendor config")
    grp.add_argument("--vendor-config", help="Path to a YAML vendor config")
    args = ap.parse_args()

    run_dir = Path(args.outdir) / f"{args.po}"
    logger = setup_logging(run_dir)
    logger.info(f"Starting run for PO {args.po}")

    try:
        df_in = pd.read_csv(args.input, dtype={"SKU": "string"}, keep_default_na=False)
        df_in["SKU"] = df_in["SKU"].astype("string").str.replace("\u200b","", regex=False).str.strip()

        df_req = map_to_required(df_in)
        # Validate schema and cross-field totals
        validated = schema().validate(df_req)
        # Transform business rules
        df_t = dedupe_and_normalize(validated)
        # Build output in Coast format
        out = to_coast(df_t)
        # Produce idempotent file name
        hh = hash_output(out)
        ts = pd.Timestamp.utcnow().strftime("%Y%m%d-%H%M")
        out_path = run_dir / f"new_coast_cart_{args.po}_{ts}_{hh}.csv"
        out.to_csv(out_path, index=False, quoting=csv.QUOTE_ALL, lineterminator="\n")

        # Optional variance flags
        flags = []
        if args.price_history:
            flags = variance_flags(df_t, Path(args.price_history))

        summary = {
            "po": args.po,
            "input_file": str(Path(args.input).resolve()),
            "output_file": str(out_path.resolve()),
            "rows_in": int(len(df_in)),
            "rows_validated": int(len(validated)),
            "rows_out": int(len(out)),
            "total_qty": int(out["Qty Ordered"].sum()),
            "total_extended_price": float(out["Extended Price"].sum()),
            "variance_flags": flags,
            "status": "OK"
        }
        with open(run_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        with open(run_dir / "summary.md", "w") as f:
            f.write(f"# PO {args.po} Summary\\n\\n")
            f.write(f"- Output: `{out_path.name}`\\n")
            f.write(f"- Rows in/out: {len(df_in)} → {len(out)}\\n")
            f.write(f"- Total Qty: {summary['total_qty']}\\n")
            f.write(f"- Total Extended: ${summary['total_extended_price']:.2f}\\n")
            if flags:
                f.write(f"- **Variance Flags** (>20% vs history): {len(flags)}\\n")

        logger.info(f"Completed PO {args.po}: {out_path.name}")
        print(json.dumps(summary, indent=2))
        sys.exit(0)

    except (pa.errors.SchemaError, KeyError, ValueError) as e:
        logger.exception("Validation failed")
        print(f"VALIDATION ERROR:\n{e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unhandled error")
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

if __name__ == "__main__":
    main()
