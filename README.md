# Stocky → Coast PO Conversion (ETL)

Converts a **Stocky** purchase-order CSV into a **Coast/Erikson** cart CSV with **schema validation**, **cross‑field checks**, **duplicate SKU handling**, and **auditable run artifacts** (summary + logs). Designed as a small, production‑style ETL you can schedule and monitor.

## Why this exists
Manual re‑entry between Stocky and vendor carts is slow and error‑prone. This tool validates and transforms the data, producing a ready‑to‑import CSV plus a summary you can review or archive.

## What this tool guarantees (Data Contract)
- **Required columns** in input:  
  `SKU (str)`, `Qty Ordered (int ≥ 0)`, `Cost (base) (float ≥ 0)`, `Total Cost (base) (float ≥ 0)`  
- **Cross‑field check** (fail‑fast): `abs(Qty * Cost - Total) < 0.01` for every row.
- **Business rules** (Transform):  
  - Duplicate SKUs are collapsed (quantities summed; totals recalculated).  
  - Money values normalized to **2 decimals**.  
  - Optional **variance flags** if you provide `price_history.csv` (flags >20% unit‑cost change).
- **Auditable outputs** (Load):  
  - Idempotent file name: `new_coast_cart_<PO>_<YYYYMMDD-HHMM>_<hash8>.csv`  
  - `summary.json` + `summary.md` with counts, totals, and flags.  
  - Rotating log file `stocky2coast.log`.  
- **Operational behavior**: Non‑zero exit if validation fails, suitable for CI/schedulers.

## Install
```bash
python -m venv .venv && source .venv/bin/activate   # (Windows: .venv\Scripts\activate)
pip install -r requirements.txt
```

## Quick demo (no access to Stocky/Coast needed)
```bash
python stocky_to_coast.py --po 1848 --input sample/po_1848.csv --price-history sample/price_history.csv
# Artifacts in: runs/1848/
# - new_coast_cart_1848_<timestamp>_<hash8>.csv
# - summary.json / summary.md
# - stocky2coast.log
```

## Inputs & Outputs
**Input CSV (Stocky)** – required columns: `SKU, Qty Ordered, Cost (base), Total Cost (base)`  
**Output CSV (Coast)** – columns: `Item Id, Qty Ordered, Unit Price, Extended Price`

## CLI usage
```bash
python stocky_to_coast.py --po <PO_NUMBER> --input <path_to_stocky_csv> [--price-history path/to/price_history.csv] [--outdir runs]
```

## Testing
```bash
pytest -q
```
Fixtures live in `tests/fixtures`. The suite covers: schema validation, cross‑field totals, SKU dedupe, and failure on bad input.

## CI (GitHub Actions)
A workflow file is provided at `.github/workflows/ci.yml` to run tests on push/PR.

## Notes
- This repo includes **synthetic sample data** for demo purposes. Replace with real PO data when running in your environment.
- Next step for interoperability: switch the **Load** step from CSV to an **API/EDI 850** submission with **ACK** capture, preserving this validation layer and run artifacts.
