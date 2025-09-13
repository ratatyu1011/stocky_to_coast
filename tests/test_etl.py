# tests/test_etl.py

import subprocess
import sys
import json
from pathlib import Path


def run_cli(args):
    return subprocess.run(
        [sys.executable, "stocky_to_coast.py"] + args,
        capture_output=True,
        text=True,
    )


def test_happy_path(tmp_path):
    outdir = tmp_path / "runs"
    res = run_cli(
        ["--po", "999", "--input", "tests/fixtures/good.csv", "--outdir", str(outdir)]
    )
    assert res.returncode == 0, res.stderr
    summary = json.loads(res.stdout)
    assert summary["rows_out"] == 2  # DEF456 was deduped
    assert summary["status"] == "OK"
    assert summary["total_qty"] == 10 + 5 + 3  # 18 total


def test_missing_required_column(tmp_path):
    outdir = tmp_path / "runs"
    res = run_cli(
        [
            "--po",
            "1000",
            "--input",
            "tests/fixtures/bad_missing_col.csv",
            "--outdir",
            str(outdir),
        ]
    )
    assert res.returncode == 1
    assert "VALIDATION ERROR" in res.stderr


def test_totals_mismatch_fails(tmp_path):
    outdir = tmp_path / "runs"
    res = run_cli(
        [
            "--po",
            "1001",
            "--input",
            "tests/fixtures/bad_totals.csv",
            "--outdir",
            str(outdir),
        ]
    )
    assert res.returncode == 1


def test_preserve_plus_in_sku(tmp_path):
    csv_in = tmp_path / "po.csv"
    csv_in.write_text(
        "SKU,Qty Ordered,Cost (base),Total Cost (base)\n" "GSP38WB+,1,16.80,16.80\n"
    )
    outdir = tmp_path / "runs"
    res = run_cli(["--po", "1010", "--input", str(csv_in), "--outdir", str(outdir)])
    assert res.returncode == 0, res.stderr

    # find the produced CSV and assert the literal value exists
    produced = list((outdir / "1010").glob("new_coast_cart_1010_*.csv"))[0]
    txt = produced.read_text()
    assert "GSP38WB+" in txt  # literal preserved


def test_soft_validate_quarantine_creates_file_and_succeeds(tmp_path):
    """Rows violating the business rule should be quarantined (not fatal) in soft-validate mode."""
    csv_in = tmp_path / "po.csv"
    # A1: bad total (1 * 10.00 != 9.99) -> quarantined
    # B2: correct total -> kept
    csv_in.write_text(
        "SKU,Qty Ordered,Cost (base),Total Cost (base)\n"
        "A1,1,10.00,9.98\n"   # 0.02 difference -> will be quarantined
        "B2,2,5.00,10.00\n"
    )
    outdir = tmp_path / "runs"
    res = run_cli(
        [
            "--po",
            "2000",
            "--input",
            str(csv_in),
            "--outdir",
            str(outdir),
            "--soft-validate",
        ]
    )
    assert res.returncode == 0, res.stderr
    summary = json.loads(res.stdout)
    assert summary["rows_quarantined"] == 1
    assert summary["rows_out"] == 1  # only B2 remains
    assert summary["status"] == "OK"

    # quarantine.csv exists with A1
    quarantine = outdir / "2000" / "quarantine.csv"
    assert quarantine.exists()
    qtxt = quarantine.read_text()
    assert "A1" in qtxt and "B2" not in qtxt

    # produced CSV exists and does not contain A1
    produced = Path(summary["output_file"])
    assert produced.exists()
    ptxt = produced.read_text()
    assert "B2" in ptxt and "A1" not in ptxt


def test_vendor_csv_decimal_formatting_respected(tmp_path):
    """
    Output decimals respect vendor config decimal_places.
    Use a custom vendor YAML with 3 decimals and verify '16.800' appears.
    """
    csv_in = tmp_path / "po.csv"
    csv_in.write_text(
        "SKU,Qty Ordered,Cost (base),Total Cost (base)\n" "Z9,1,16.80,16.80\n"
    )

    vendor_yml = tmp_path / "vendor.yml"
    vendor_yml.write_text(
        """
name: test_vendor
output:
  columns: ["Item Id", "Qty Ordered", "Unit Price", "Extended Price"]
  delimiter: ","
  decimal_places: 3
  quoting: "all"
"""
    )

    outdir = tmp_path / "runs"
    res = run_cli(
        [
            "--po",
            "3000",
            "--input",
            str(csv_in),
            "--outdir",
            str(outdir),
            "--vendor-config",
            str(vendor_yml),
        ]
    )
    assert res.returncode == 0, res.stderr
    summary = json.loads(res.stdout)

    produced = Path(summary["output_file"])
    txt = produced.read_text().splitlines()
    # Header fully quoted (quoting: all)
    assert txt[0] == '"Item Id","Qty Ordered","Unit Price","Extended Price"'
    # Three decimals enforced by vendor config
    assert '"16.800"' in txt[1]
