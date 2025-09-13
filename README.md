# Stocky → Coast / Erikson PO Conversion (ETL)

Converts a **Stocky** purchase‑order CSV into a vendor‑specific cart CSV (Coast, Erikson Music, Erikson Audio), with **schema validation**, **cross‑field checks**, **duplicate SKU handling**, and **auditable artifacts** (summary, lineage, logs). Designed as a small, production‑style ETL you can schedule and monitor.

---

## Why this exists

Manual re‑entry between Stocky and vendor carts is slow and error‑prone. This tool validates and transforms the data, producing a ready‑to‑import CSV plus a detailed summary and lineage file you can archive for auditability.

---

## Highlights

- **Config‑driven multi‑vendor output** via simple YAML files (delimiter, quoting, decimal places, column order).
- **Strict validation** of input schema and vendor config (Pydantic + Pandera).
- **Soft‑validation mode** to **quarantine** bad rows instead of failing the entire job.
- Optional **SKU format regex** (CLI or per vendor config).
- **Lineage artifact** that maps input rows → output SKUs.
- **Idempotent file naming** includes vendor name in the content hash.
- **CI‑friendly**: non‑zero exit on hard validation failures; JSON summary for programmatic consumption.

---

## Data Contract (Input → Output)

**Input (Stocky CSV)** must include these columns (case‑insensitive aliases accepted):

- `SKU` _(str)_
- `Qty Ordered` _(int ≥ 0)_
- `Cost (base)` _(float ≥ 0)_
- `Total Cost (base)` _(float ≥ 0)_

**Business rule (row‑level):**

```
abs(Qty Ordered * Cost (base) - Total Cost (base)) <= 0.01
```

**Transform (business rules):**

- Collapse duplicate SKUs; **sum quantities**; recompute totals.
- Normalize money values to configured **decimal places** (defaults to 2).

**Output (vendor cart CSV):**

- Columns (order enforced by vendor config):
  - `Item Id`, `Qty Ordered`, `Unit Price`, `Extended Price`
- CSV formatting per vendor config (delimiter, quoting, decimals).

---

## Install

```bash
python -m venv .venv
source .venv/bin/activate           # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Requirements** (key):

- `pandas`, `numpy`, `pandera`, `pyyaml`, `pydantic`

---

## Quick demo (no access to Stocky/Coast needed)

```bash
python stocky_to_coast.py --po 1848 --input sample/po_1848.csv --price-history sample/price_history.csv
# Artifacts in: runs/1848/
# - new_coast_cart_1848_<YYYYMMDD-HHMM>_<hash8>.csv
# - summary.json / summary.md
# - lineage.json
# - stocky2coast.log
```

---

## CLI

```bash
python stocky_to_coast.py   --po <PO_NUMBER>   --input <path_to_stocky_csv>   [--vendor coast|erikson_music|erikson_audio | --vendor-config path/to/custom.yml]   [--price-history path/to/price_history.csv]   [--outdir runs]   [--soft-validate]   [--sku-pattern "^[A-Za-z0-9+\-_.]+$"]
```

**Flags:**

- `--vendor` _(or `--vendor-config`)_: selects the output formatting (columns, delimiter, quoting, decimals).
- `--soft-validate`: quarantines rows that violate the business rule to `quarantine.csv` instead of failing the run.
- `--sku-pattern`: optional regex for SKU validation; **overrides** any vendor config pattern.
- `--price-history`: optional CSV with columns `SKU,LastCost` to flag >20% unit‑cost changes.

---

## Vendor configuration (YAML)

Place YAML files under `vendor_configs/`. Example:

```yaml
# vendor_configs/coast.yml
name: coast
output:
  columns: ["Item Id", "Qty Ordered", "Unit Price", "Extended Price"]
  delimiter: ","
  decimal_places: 2
  quoting: "all"
input:
  # Optional SKU regex (allows letters, numbers, plus, dash, underscore, dot)
  sku_pattern: "^[A-Za-z0-9+\-_.]+$"
```

> **Validation:** The config is validated with Pydantic.  
> `output.columns` must be exactly `["Item Id","Qty Ordered","Unit Price","Extended Price"]`.  
> `quoting` ∈ `{all, minimal, nonnumeric, none}`.

---

## Artifacts per run

All artifacts live under `runs/<PO>/`:

- `new_coast_cart_<PO>_<timestamp>_<hash8>.csv` – ready to import
- `summary.json` – machine‑readable run summary
- `summary.md` – human‑readable run summary
- `lineage.json` – input row IDs grouped by SKU that made it to output
- `stocky2coast.log` – rotating run logs
- `quarantine.csv` – **only** in `--soft-validate` mode when rows are quarantined

**Summary contents (excerpt):**

```json
{
  "po": "1848",
  "vendor": "coast",
  "mode": "strict",
  "rows_in": 42,
  "rows_validated": 42,
  "rows_quarantined": 0,
  "rows_out": 40,
  "total_qty": 123,
  "total_extended_price": 4567.89,
  "variance_flags": []
}
```

---

## Testing

```bash
pytest -q
```

The suite covers:

- Happy path (dedupe + totals)
- Missing column (hard fail)
- Totals mismatch (hard fail)
- **SKU character preservation** (e.g., `+`)
- **Soft‑validate** quarantine succeeds and produces `quarantine.csv`
- **Vendor formatting respected** (decimal places, quoting)

---

## CI (GitHub Actions)

A workflow at `.github/workflows/ci.yml` runs the test suite on push/PR.  
**Nice extras to consider** (already supported by the project):

- Python version matrix (3.10–3.13)
- Linting (`ruff`, `black --check`)
- Upload `runs/**/summary.json` as artifacts for smoke jobs

---

## Troubleshooting

- **“Missing required column”**: Your Stocky export may use a synonym (e.g., _Extended Price_). Header aliases are supported; verify exact spelling/casing.
- **“Row total mismatch”**: If small rounding differences are common in your data, run with `--soft-validate` to quarantine and review.
- **Vendor import rejects SKUs with symbols**: Ensure the **SKU regex** allows the symbols you need (e.g., `+`) either via `--sku-pattern` or the vendor YAML.

---

## Security

- Do **not** commit real POs or secrets.
- Use `.env`/secrets in CI as needed.
- Vendor configs should not contain credentials—this tool handles **data shaping**, not auth to vendor portals.

---

## Roadmap

- Replace CSV “Load” with **API/EDI 850** submission and **ACK capture**, keeping this validation and audit layer intact.
- Add richer **variance analytics** and configurable thresholds per vendor.

---

## License

MIT
