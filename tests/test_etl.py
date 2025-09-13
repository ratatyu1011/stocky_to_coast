# tests/test_etl.py

import subprocess, sys, json, os, tempfile, shutil, pathlib

def run_cli(args):
    return subprocess.run([sys.executable, "stocky_to_coast.py"] + args, capture_output=True, text=True)

def test_happy_path(tmp_path):
    outdir = tmp_path / "runs"
    res = run_cli(["--po","999","--input","tests/fixtures/good.csv","--outdir",str(outdir)])
    assert res.returncode == 0, res.stderr
    summary = json.loads(res.stdout)
    assert summary["rows_out"] == 2  # DEF456 was deduped
    assert summary["status"] == "OK"
    assert summary["total_qty"] == 10 + 5 + 3  # 18 total

def test_missing_required_column(tmp_path):
    outdir = tmp_path / "runs"
    res = run_cli(["--po","1000","--input","tests/fixtures/bad_missing_col.csv","--outdir",str(outdir)])
    assert res.returncode == 1
    assert "VALIDATION ERROR" in res.stderr


def test_totals_mismatch_fails(tmp_path):
    outdir = tmp_path / "runs"
    res = run_cli(["--po","1001","--input","tests/fixtures/bad_totals.csv","--outdir",str(outdir)])
    assert res.returncode == 1

def test_preserve_plus_in_sku(tmp_path):
    csv_in = tmp_path / "po.csv"
    csv_in.write_text(
        "SKU,Qty Ordered,Cost (base),Total Cost (base)\n"
        "GSP38WB+,1,16.80,16.80\n"
    )
    outdir = tmp_path / "runs"
    res = run_cli(["--po","1010","--input",str(csv_in),"--outdir",str(outdir)])
    assert res.returncode == 0, res.stderr

    # find the produced CSV and assert the literal value exists
    produced = list((outdir / "1010").glob("new_coast_cart_1010_*.csv"))[0]
    txt = produced.read_text()
    assert "GSP38WB+" in txt  # literal preserved
