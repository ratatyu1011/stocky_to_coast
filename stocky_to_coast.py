# stocky_to_coast.py
import argparse
import sys
import hashlib
import json
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import List, Literal, Optional

import csv
import yaml
import pandas as pd
import numpy as np
import pandera.pandas as pa
from pandera.pandas import Column, DataFrameSchema, Check
from pydantic import BaseModel, Field, ValidationError


# -----------------------------
# Canonical columns & defaults
# -----------------------------
IN_COLS = ["SKU", "Qty Ordered", "Cost (base)", "Total Cost (base)"]
OUT_COLS = ["Item Id", "Qty Ordered", "Unit Price", "Extended Price"]

# Header aliases for Stocky exports (case-insensitive)
STOCKY_ALIASES = {
    "SKU": ["SKU", "Sku", "Item Id", "ItemID"],
    "Qty Ordered": ["Qty Ordered", "Quantity Ordered", "Qty"],
    "Cost (base)": ["Cost (base)", "Unit Cost (base)", "Unit Cost", "Cost"],
    "Total Cost (base)": ["Total Cost (base)", "Extended Price", "Total"],
}

# Default vendor config (overridable by --vendor or --vendor-config)
DEFAULT_VENDOR_CFG = {
    "name": "default",
    "output": {
        "columns": ["Item Id", "Qty Ordered", "Unit Price", "Extended Price"],
        "delimiter": ",",
        "decimal_places": 2,
        "quoting": "all",
    },
    # optional input constraints (e.g., SKU regex)
    "input": {
        # "sku_pattern": r"^[A-Za-z0-9+\-_.]+$"
    },
}

QUOTING_MAP = {
    "all": csv.QUOTE_ALL,
    "minimal": csv.QUOTE_MINIMAL,
    "nonnumeric": csv.QUOTE_NONNUMERIC,
    "none": csv.QUOTE_NONE,
}


# -----------------------------
# Pydantic models for config validation
# -----------------------------
class OutputCfg(BaseModel):
    columns: List[str]
    delimiter: str = ","
    decimal_places: int = Field(default=2, ge=0, le=6)
    quoting: Literal["all", "minimal", "nonnumeric", "none"] = "all"


class InputCfg(BaseModel):
    sku_pattern: Optional[str] = None


class VendorCfg(BaseModel):
    name: str
    output: OutputCfg
    input: Optional[InputCfg] = InputCfg()


def validate_vendor_cfg(raw_cfg: dict) -> VendorCfg:
    try:
        cfg = VendorCfg.model_validate(raw_cfg)
    except ValidationError as e:
        raise ValueError(f"Invalid vendor config: {e}") from e
    # Ensure output columns exactly match expected set (order is validated at write)
    if set(cfg.output.columns) != set(OUT_COLS):
        raise ValueError(f"output.columns must contain exactly {OUT_COLS}")
    return cfg


# -----------------------------
# Helpers
# -----------------------------
def _deep_update(dst: dict, src: dict) -> dict:
    """Recursively merge src into dst (dst is mutated and also returned)."""
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst


def load_vendor_cfg(vendor: Optional[str], vendor_config_path: Optional[str]) -> dict:
    """Load vendor config dict either by built-in name or explicit path, merged over defaults."""
    cfg = dict(DEFAULT_VENDOR_CFG)  # shallow copy of defaults
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
    """Map a Stocky CSV (with many columns) to the canonical input columns."""
    cols = {}
    lower_map = {c.lower(): c for c in df.columns}

    for canonical, aliases in STOCKY_ALIASES.items():
        real = None
        for a in aliases:
            if a in df.columns:
                real = a
                break
            cand = lower_map.get(a.lower())
            if cand:
                real = cand
                break
        if real:
            cols[canonical] = df[real]
        else:
            raise KeyError(f"Missing required column: {canonical}")

    out = pd.DataFrame(cols)
    # dtypes – strict; let errors surface as validation failures
    out["Qty Ordered"] = pd.to_numeric(out["Qty Ordered"], errors="raise").astype(int)
    for c in ["Cost (base)", "Total Cost (base)"]:
        out[c] = pd.to_numeric(out[c], errors="raise")
    # SKU as string; preserve literal symbols (e.g., '+'); remove zero-widths; trim
    out["SKU"] = out["SKU"].astype("string").str.replace("\u200b", "", regex=False).str.strip()
    return out


def setup_logging(run_dir: Path):
    run_dir.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("stocky2coast")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = RotatingFileHandler(run_dir / "stocky2coast.log", maxBytes=200_000, backupCount=3)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


def _sku_check_for_pattern(pattern: Optional[str]):
    if not pattern:
        return None
    # Prefer Check.str_matches if available; fallback to vectorized .str.match
    try:
        return Check.str_matches(pattern)
    except AttributeError:
        return Check(
            lambda s: s.str.match(pattern).fillna(False),  # type: ignore[attr-defined]
            element_wise=False,
            error="SKU pattern mismatch",
        )


def schema(include_business: bool = True, sku_pattern: Optional[str] = None):
    sku_checks = []
    sk = _sku_check_for_pattern(sku_pattern)
    if sk:
        sku_checks.append(sk)

    columns = {
        "SKU": Column(str, checks=sku_checks or None, nullable=False),
        "Qty Ordered": Column(int, Check.ge(0), nullable=False),
        "Cost (base)": Column(float, Check.ge(0.0), nullable=False),
        "Total Cost (base)": Column(float, Check.ge(0.0), nullable=False),
    }

    checks = []
    if include_business:
        checks.append(
            Check(
                lambda df: ((df["Qty Ordered"] * df["Cost (base)"]) - df["Total Cost (base)"]).abs() <= 0.01,
                error="Row total mismatch: |Qty*Cost - Total| > 0.01",
            )
        )

    return DataFrameSchema(columns, checks=checks or None, strict=True, coerce=True)


def dedupe_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Collapse duplicate SKUs; sum quantities; recompute totals and round to cents."""
    g = df.groupby("SKU", as_index=False).agg(
        {"Qty Ordered": "sum", "Cost (base)": "first"}  # assume unit cost constant per SKU within a PO
    )
    g["Total Cost (base)"] = (g["Qty Ordered"] * g["Cost (base)"]).round(2)
    for c in ["Cost (base)", "Total Cost (base)"]:
        g[c] = g[c].round(2)
    return g


def to_coast(df: pd.DataFrame) -> pd.DataFrame:
    """Transform canonical dataframe to vendor-agnostic cart schema."""
    out = pd.DataFrame(
        {
            "Item Id": df["SKU"],
            "Qty Ordered": df["Qty Ordered"],
            "Unit Price": df["Cost (base)"].round(2),
            "Extended Price": (df["Qty Ordered"] * df["Cost (base)"]).round(2),
        }
    )
    return out[OUT_COLS]


def hash_output(df: pd.DataFrame, vendor_name: str) -> str:
    payload = (vendor_name + "\n").encode("utf-8") + df.to_csv(index=False).encode("utf-8")
    return hashlib.md5(payload).hexdigest()[:8]


def variance_flags(df: pd.DataFrame, price_hist_path: Path, threshold=0.20):
    """Optional: flag >20% price variance vs. history."""
    if not price_hist_path or not price_hist_path.exists():
        return []
    hist = pd.read_csv(price_hist_path)  # expected: columns SKU, LastCost
    if "LastCost" not in hist.columns or "SKU" not in hist.columns:
        return []
    hist = hist[hist["LastCost"] > 0]
    merged = df.merge(hist, on="SKU", how="left")
    merged["pct_change"] = (merged["Cost (base)"] - merged["LastCost"]) / merged["LastCost"]
    flg = merged[(merged["LastCost"].notna()) & (merged["pct_change"].abs() > threshold)]
    return flg[["SKU", "LastCost", "Cost (base)", "pct_change"]].to_dict(orient="records")


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser(description="Convert Stocky PO CSV to Vendor cart CSV with validation.")
    ap.add_argument("--po", required=True, help="PO number, e.g. 1848")
    ap.add_argument("--input", required=True, help="Path to Stocky CSV (po_XXXX.csv)")
    ap.add_argument("--outdir", default="runs", help="Output directory for artifacts")
    ap.add_argument("--price-history", default="", help="Optional price_history.csv with columns SKU,LastCost")
    ap.add_argument("--soft-validate", action="store_true",
                    help="Quarantine rows failing business rules instead of failing the run")
    ap.add_argument("--sku-pattern", default="",
                    help="Optional regex to validate SKU format; overrides vendor config if provided")
    grp = ap.add_mutually_exclusive_group(required=False)
    grp.add_argument(
        "--vendor",
        choices=["coast", "erikson_music", "erikson_audio"],
        help="Built-in vendor config (vendor_configs/<name>.yml)",
    )
    grp.add_argument("--vendor-config", help="Path to a YAML vendor config")
    args = ap.parse_args()

    run_dir = Path(args.outdir) / f"{args.po}"
    logger = setup_logging(run_dir)
    logger.info(f"Starting run for PO {args.po}")

    try:
        # Read raw input
        df_in = pd.read_csv(args.input, dtype={"SKU": "string"}, keep_default_na=False)
        # Clean SKU (remove zero-width, trim) but preserve literal symbols like '+'
        df_in["SKU"] = df_in["SKU"].astype("string").str.replace("\u200b", "", regex=False).str.strip()

        # Reduce to canonical input columns & normalize dtypes
        df_req = map_to_required(df_in)
        # Keep a lineage copy for mapping input rows -> SKU groups
        lineage_source = df_req.copy()
        lineage_source["__row_id"] = lineage_source.index.astype(int)

        # Load and validate vendor config
        raw_cfg = load_vendor_cfg(args.vendor, args.vendor_config)
        vcfg = validate_vendor_cfg(raw_cfg)

        # SKU pattern preference: CLI overrides vendor config
        sku_pattern = args.sku_pattern or (vcfg.input.sku_pattern if vcfg.input and vcfg.input.sku_pattern else None)

        # Validate schema (strict types/columns). In soft mode, skip business rule here.
        validated = schema(include_business=not args.soft_validate, sku_pattern=sku_pattern).validate(df_req)

        # Optionally quarantine rows that violate the business rule (cross-field totals)
        rows_quarantined = 0
        if args.soft_validate:
            mask = ((validated["Qty Ordered"] * validated["Cost (base)"]) - validated["Total Cost (base)"]).abs() <= 0.01
            bad = validated.loc[~mask]
            rows_quarantined = int(len(bad))
            if rows_quarantined > 0:
                bad_path = run_dir / "quarantine.csv"
                bad.to_csv(bad_path, index=False)
                logger.warning("Quarantined %d invalid rows to %s", rows_quarantined, bad_path.name)
            validated = validated.loc[mask]

        # Transform (dedupe, normalize)
        df_t = dedupe_and_normalize(validated)

        # Build vendor-agnostic output
        out = to_coast(df_t)

        # Apply vendor formatting & column order
        out_cols = vcfg.output.columns
        sep = vcfg.output.delimiter
        dp = int(vcfg.output.decimal_places)
        quoting_val = QUOTING_MAP[vcfg.output.quoting]

        out = out[out_cols]  # reorder/select columns

        # Idempotent file name (vendor + data in hash)
        hh = hash_output(out, vcfg.name)
        ts = pd.Timestamp.utcnow().strftime("%Y%m%d-%H%M")
        out_path = run_dir / f"new_coast_cart_{args.po}_{ts}_{hh}.csv"

        # Write CSV with explicit quoting and fixed decimal places
        out.to_csv(
            out_path,
            index=False,
            sep=sep,
            quoting=quoting_val,
            float_format=f"%.{dp}f",
            lineterminator="\n",
        )

        # Optional variance flags
        flags = []
        if args.price_history:
            flags = variance_flags(df_t, Path(args.price_history))

        # Lineage artifact: input row ids -> SKU groups (post-validation, pre-dedupe)
        # Use only rows that made it to 'validated'
        valid_idx = set(validated.index.tolist())
        lineage_valid = lineage_source.loc[lineage_source.index.isin(valid_idx)]
        lineage = (
            lineage_valid.groupby("SKU")["__row_id"]
            .apply(lambda s: s.astype(int).tolist())
            .reset_index()
        )
        with open(run_dir / "lineage.json", "w") as f:
            json.dump(lineage.to_dict(orient="records"), f, indent=2)

        # Run summary
        summary = {
            "po": args.po,
            "vendor": vcfg.name,
            "mode": "soft-validate" if args.soft_validate else "strict",
            "sku_pattern": sku_pattern or None,
            "input_file": str(Path(args.input).resolve()),
            "output_file": str(out_path.resolve()),
            "rows_in": int(len(df_in)),
            "rows_validated": int(len(validated) + rows_quarantined),
            "rows_quarantined": rows_quarantined,
            "rows_out": int(len(out)),
            "total_qty": int(out["Qty Ordered"].sum()),
            "total_extended_price": float(out["Extended Price"].sum()),
            "variance_flags": flags,
            "status": "OK",
        }
        with open(run_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        with open(run_dir / "summary.md", "w") as f:
            f.write(f"# PO {args.po} Summary\n\n")
            f.write(f"- Vendor: `{vcfg.name}`\n")
            f.write(f"- Mode: `{summary['mode']}`\n")
            f.write(f"- Output: `{out_path.name}`\n")
            f.write(f"- Rows in/out: {len(df_in)} → {len(out)} (quarantined: {rows_quarantined})\n")
            f.write(f"- Total Qty: {summary['total_qty']}\n")
            f.write(f"- Total Extended: ${summary['total_extended_price']:.2f}\n")
            if flags:
                f.write(f"- **Variance Flags** (>20% vs history): {len(flags)}\n")

        logger.info("Completed PO %s for vendor %s: %s", args.po, vcfg.name, out_path.name)
        print(json.dumps(summary, indent=2))
        sys.exit(0)

    except (pa.errors.SchemaError, KeyError, ValueError) as e:
        # Treat header/typing/validation issues as validation failures (exit 1)
        logger.exception("Validation failed")
        print(f"VALIDATION ERROR:\n{e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        logger.exception("Unhandled error")
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
