# -*- coding: utf-8 -*-
"""一括ダウンロードの取り込み（services/bulk_import.py）のテスト"""
import glob
import gzip
import os

import pandas as pd
import pytest


def _write_gz(root, rel_path, df):
    path = os.path.join(str(root), *rel_path.split("/"))
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8", newline="") as f:
        df.to_csv(f, index=False)
    return path


def test_bulk_files_orders_monthly_before_daily_of_same_month(tmp_path):
    from services.bulk_import import bulk_files

    empty = pd.DataFrame({"x": [1]})
    _write_gz(tmp_path, "fins/summary/live/fins_summary_20260902.csv.gz", empty)
    _write_gz(tmp_path, "fins/summary/historical/2026/fins_summary_202609.csv.gz", empty)
    _write_gz(tmp_path, "fins/summary/historical/2026/fins_summary_202608.csv.gz", empty)
    _write_gz(tmp_path, "fins/summary/live/fins_summary_20260901.csv.gz", empty)

    names = [os.path.basename(p) for p in bulk_files("fins/summary", str(tmp_path))]
    assert names == ["fins_summary_202608.csv.gz", "fins_summary_202609.csv.gz",
                     "fins_summary_20260901.csv.gz", "fins_summary_20260902.csv.gz"]


def test_import_fins_bulk_wins_and_keeps_cache_only_rows(tmp_path):
    from services.bulk_import import import_fins

    cols = ["DiscDate", "Code", "DiscNo", "DocType", "Sales", "ShEq"]
    cache = pd.DataFrame([
        ["2021-05-10", "10000", "A", "FYFinancialStatements_Consolidated_JP", "100", ""],   # 訂正前
        ["2021-05-10", "20000", "B", "FYFinancialStatements_Consolidated_JP", "50", ""],    # 手元にしかない
    ], columns=cols)
    dest = tmp_path / "fins_cache.parquet"
    cache.to_parquet(dest, index=False)

    _write_gz(tmp_path / "bulk", "fins/summary/historical/2021/fins_summary_202105.csv.gz", pd.DataFrame([
        ["2021-05-10", "10000", "A", "FYFinancialStatements_Consolidated_JP", "120", "900"],  # 訂正後・自己資本あり
        ["2021-05-11", "30000", "C", "FYFinancialStatements_Consolidated_JP", "70", "300"],   # 手元に無かった開示
    ], columns=cols))

    result, stats = import_fins(str(dest), root=str(tmp_path / "bulk"))
    by_no = result.set_index("DiscNo")

    assert by_no.loc["A", "Sales"] == "120" and by_no.loc["A", "ShEq"] == "900"
    assert by_no.loc["B", "Sales"] == "50"
    assert by_no.loc["C", "Code"] == "30000"
    assert stats["added"] == 1 and stats["updated"] == 1 and stats["cache_only"] == 1
    assert os.path.exists(stats["backup"])
    assert len(pd.read_parquet(stats["backup"])) == 2                     # バックアップは統合前の内容
    assert set(pd.read_parquet(dest)["DiscNo"]) == {"A", "B", "C"}


def test_import_valuation_prefers_newer_file_and_casts_numbers(tmp_path):
    from services.bulk_import import import_valuation

    cols = ["Date", "Code", "EPS", "FwdEPS", "BPS", "ROE", "FwdROE", "PER", "FwdPER", "PBR", "MktCap"]
    _write_gz(tmp_path / "bulk", "equities/valuation/historical/2026/equities_valuation_202609.csv.gz",
              pd.DataFrame([["2026-09-01", "10000", "10", "11", "100", "0.1", "0.11", "10", "9", "1", "1000"]], columns=cols))
    _write_gz(tmp_path / "bulk", "equities/valuation/live/equities_valuation_20260901.csv.gz",
              pd.DataFrame([["2026-09-01", "10000", "10", "12", "100", "0.1", "0.12", "10", "8", "1", "1000"],
                            ["2026-09-01", "13050", "", "", "", "", "", "", "", "", ""]], columns=cols))   # ETF は全項目空欄

    out = import_valuation(str(tmp_path / "valuation.parquet"), root=str(tmp_path / "bulk"))
    row = out.set_index("Code").loc["10000"]
    assert row["FwdPER"] == 8.0                   # 日次（新しい側）を優先
    assert out["FwdPER"].dtype.kind == "f"
    assert pd.isna(out.set_index("Code").loc["13050", "PER"])
