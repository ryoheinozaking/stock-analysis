# -*- coding: utf-8 -*-
"""
fins_cache レコード選別バグの回帰テスト

背景: CurPerType=='FY' には決算短信（実績）だけでなく
EarnForecastRevision / DividendForecastRevision（実績列が空）が混入する。
また同一決算期の訂正短信が重複レコードとして残る。
これらが最新側に来ると、ハードフィルタ除外・成長率/増配判定の破壊が起きる。
"""
import os

import numpy as np
import pandas as pd
import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_FINS_PARQUET = os.path.join(_ROOT, "data", "fins_cache.parquet")


# ════════════════════════════════════════════════════════════════════════
#  合成データヘルパー
# ════════════════════════════════════════════════════════════════════════

_STMT = "FYFinancialStatements_Consolidated_JP"
_EARN_REV = "EarnForecastRevision"
_DIV_REV = "DividendForecastRevision"


def _fins_row(code, disc_date, doctype, fy_en, per_type="FY", **vals):
    row = {
        "Code": code,
        "DiscDate": disc_date,
        "DocType": doctype,
        "CurPerType": per_type,
        "CurFYEn": fy_en,
        # 実績列（決算短信のみ値あり）
        "Sales": np.nan, "OP": np.nan, "NP": np.nan, "EPS": np.nan,
        "BPS": np.nan, "Eq": np.nan, "EqAR": np.nan, "TA": np.nan,
        "CFO": np.nan, "ShOutFY": np.nan, "DivAnn": np.nan,
        # 予想列
        "FSales": np.nan, "FOP": np.nan, "FNP": np.nan, "FEPS": np.nan,
        "FDivAnn": np.nan, "NxFSales": np.nan, "NxFNp": np.nan,
        "NxFEPS": np.nan, "NxFDivAnn": np.nan,
    }
    row.update(vals)
    return row


def _fins_df(rows):
    """_load_fins_fy の出力と同じ並び（Code 昇順・DiscDate 降順）で返す。"""
    df = pd.DataFrame(rows)
    df["DiscDate"] = pd.to_datetime(df["DiscDate"])
    return df.sort_values(["Code", "DiscDate"],
                          ascending=[True, False]).reset_index(drop=True)


def _prices_df(code, n=40, start="2025-01-06", base=1000.0):
    dates = pd.bdate_range(start, periods=n)
    return pd.DataFrame({
        "Date": dates.strftime("%Y-%m-%d"),
        "Code": code,
        "O": base, "H": base * 1.01, "L": base * 0.99,
        "C": [base + i for i in range(n)],
        "Vo": 100000.0,
        "AdjFactor": 1.0,
    })


# ════════════════════════════════════════════════════════════════════════
#  fins_utils: レコード選別ユーティリティ
# ════════════════════════════════════════════════════════════════════════

def test_filter_fy_statements_drops_revisions():
    from services.fins_utils import filter_fy_statements

    df = _fins_df([
        _fins_row("10000", "2025-05-10", _STMT, "2025-03-31", Sales=1000.0),
        _fins_row("10000", "2025-08-01", _EARN_REV, "2026-03-31"),
        _fins_row("10000", "2025-09-01", _DIV_REV, "2026-03-31"),
        _fins_row("10000", "2025-08-05", "1QFinancialStatements_Consolidated_JP",
                  "2026-03-31", per_type="1Q", Sales=250.0),
    ])
    out = filter_fy_statements(df)
    assert len(out) == 1
    assert out.iloc[0]["DocType"] == _STMT


def test_dedupe_same_fy_keeps_latest_correction():
    from services.fins_utils import dedupe_same_fy

    df = _fins_df([
        # FY2025: 訂正（6月）が本決算（5月）を上書きすべき
        _fins_row("10000", "2025-05-10", _STMT, "2025-03-31", NP=100.0),
        _fins_row("10000", "2025-06-20", _STMT, "2025-03-31", NP=120.0),
        _fins_row("10000", "2024-05-10", _STMT, "2024-03-31", NP=80.0),
    ])
    out = dedupe_same_fy(df)
    assert len(out) == 2
    # 並びは Code 昇順・DiscDate 降順を維持
    assert float(out.iloc[0]["NP"]) == 120.0   # 訂正側が残る
    assert float(out.iloc[1]["NP"]) == 80.0

    # CurFYEn が欠損している行は dedup の巻き添えで消えない
    df2 = _fins_df([
        _fins_row("10000", "2025-05-10", _STMT, np.nan, NP=1.0),
        _fins_row("10000", "2024-05-10", _STMT, np.nan, NP=2.0),
    ])
    assert len(dedupe_same_fy(df2)) == 2


# ════════════════════════════════════════════════════════════════════════
#  pipeline_service._build_fins_metrics
# ════════════════════════════════════════════════════════════════════════

def test_build_fins_metrics_handles_correction_duplicates():
    """訂正短信の重複があっても「前期」は真の前年度と比較する。"""
    from services.pipeline_service import _build_fins_metrics

    fins = _fins_df([
        # FY2025 訂正（最新）
        _fins_row("10000", "2025-06-20", _STMT, "2025-03-31",
                  Sales=1000.0, OP=100.0, EPS=50.0, EqAR=0.5,
                  ShOutFY=1_000_000.0, DivAnn=20.0),
        # FY2025 オリジナル（同一決算期の重複）
        _fins_row("10000", "2025-05-10", _STMT, "2025-03-31",
                  Sales=1000.0, OP=100.0, EPS=50.0, EqAR=0.5,
                  ShOutFY=1_000_000.0, DivAnn=20.0),
        # FY2024
        _fins_row("10000", "2024-05-10", _STMT, "2024-03-31",
                  Sales=900.0, OP=80.0, EPS=40.0, EqAR=0.5,
                  ShOutFY=1_000_000.0, DivAnn=15.0),
        # FY2023
        _fins_row("10000", "2023-05-10", _STMT, "2023-03-31",
                  Sales=800.0, OP=70.0, EPS=30.0, EqAR=0.5,
                  ShOutFY=1_000_000.0, DivAnn=10.0),
    ])
    prices = _prices_df("10000")

    m = _build_fins_metrics(fins, prices)
    row = m[m["code"] == "10000"].iloc[0]

    # eps_growth は FY2025(50) vs FY2024(40) = +25%（重複比較なら 0% になる）
    assert row["eps_growth"] == pytest.approx(25.0)
    # 増配は 10 → 15 → 20 で 2 期連続（重複比較なら 0 になる）
    assert row["div_trend"] == 2
    # 営業益は 70 → 80 → 100 で 2 期連続増
    assert row["op_trend"] == 2
    assert not row["op_turnaround"]


def test_op_trend_flat_is_not_an_increase():
    """営業益が完全横ばいなら op_trend=0（コメント上の仕様: 横ばい/減=0）。"""
    from services.pipeline_service import _build_fins_metrics

    fins = _fins_df([
        _fins_row("20000", "2025-05-10", _STMT, "2025-03-31",
                  Sales=1000.0, OP=100.0, EPS=50.0),
        _fins_row("20000", "2024-05-10", _STMT, "2024-03-31",
                  Sales=1000.0, OP=100.0, EPS=50.0),
        _fins_row("20000", "2023-05-10", _STMT, "2023-03-31",
                  Sales=900.0, OP=90.0, EPS=45.0),
    ])
    m = _build_fins_metrics(fins, _prices_df("20000"))
    row = m[m["code"] == "20000"].iloc[0]
    assert row["op_trend"] == 0
    assert not row["op_turnaround"]


def test_op_turnaround_v_shape_detected():
    """前期減益 → 今期回復は op_turnaround=True（V字転換 +15pt の前提）。"""
    from services.pipeline_service import _build_fins_metrics

    fins = _fins_df([
        _fins_row("30000", "2025-05-10", _STMT, "2025-03-31",
                  Sales=1000.0, OP=100.0, EPS=50.0),
        _fins_row("30000", "2024-05-10", _STMT, "2024-03-31",
                  Sales=900.0, OP=80.0, EPS=40.0),
        _fins_row("30000", "2023-05-10", _STMT, "2023-03-31",
                  Sales=950.0, OP=90.0, EPS=45.0),
    ])
    m = _build_fins_metrics(fins, _prices_df("30000"))
    row = m[m["code"] == "30000"].iloc[0]
    assert row["op_trend"] == 1
    assert row["op_turnaround"]


# ════════════════════════════════════════════════════════════════════════
#  pipeline_service._load_fins_fy（実データ統合テスト）
# ════════════════════════════════════════════════════════════════════════

@pytest.mark.skipif(not os.path.exists(_FINS_PARQUET),
                    reason="fins_cache.parquet がローカルにない")
def test_load_fins_fy_returns_statements_only():
    from services.pipeline_service import _load_fins_fy

    fy = _load_fins_fy()
    assert len(fy) > 0
    # 予想修正レコードが混入していないこと
    assert not fy["DocType"].astype(str).str.contains("Revision").any()
    # 全レコードが FY 決算短信であること
    assert fy["DocType"].astype(str).str.startswith("FYFinancialStatements").all()


# ════════════════════════════════════════════════════════════════════════
#  run_pipeline の Top-N 選定（バックテストとの整合）
# ════════════════════════════════════════════════════════════════════════

def test_select_top_candidates_ranks_by_total_score():
    """Top-N はシグナル優先順位ではなく total_score 純順位で選ぶ
    （backtest_value_service の検証と同一基準）。"""
    from services.pipeline_service import select_top_candidates

    # calc_trade_signals 後の並び（BUY 先頭）を模倣
    scored = pd.DataFrame({
        "code":        ["A", "B", "C", "D"],
        "total_score": [62.0, 61.0, 90.0, 55.0],
        "signal":      ["BUY", "BUY", "WATCH", "WATCH"],
    })
    top2 = select_top_candidates(scored, 2)
    assert list(top2["code"]) == ["C", "A"]
    assert list(top2["total_score"]) == [90.0, 62.0]


# ════════════════════════════════════════════════════════════════════════
#  batch_service._compute_metrics（stock_cache 側）
# ════════════════════════════════════════════════════════════════════════

def test_compute_metrics_survives_revision_as_latest_record():
    """最新レコードが配当予想修正（実績列が空）でも、
    実績は直近の決算短信から計算され PBR/ROE/op_positive が壊れない。"""
    from services.batch_service import _compute_metrics

    code = "13790"
    fins = _fins_df([
        # 最新: 配当予想修正（実績列はすべて空）
        _fins_row(code, "2026-02-13", _DIV_REV, "2026-03-31", FDivAnn=55.0),
        # FY2025 本決算
        _fins_row(code, "2025-05-14", _STMT, "2025-03-31",
                  Sales=1000.0, OP=100.0, NP=70.0, EPS=50.0,
                  Eq=500.0, EqAR=0.5, TA=1000.0, CFO=90.0,
                  ShOutFY=1_000_000.0, DivAnn=50.0),
        # FY2024 本決算
        _fins_row(code, "2024-05-14", _STMT, "2024-03-31",
                  Sales=900.0, OP=80.0, NP=60.0, EPS=40.0,
                  Eq=450.0, EqAR=0.5, TA=900.0, CFO=80.0,
                  ShOutFY=1_000_000.0, DivAnn=40.0),
    ])
    prices = _prices_df(code)

    row = _compute_metrics(code, prices, fins, None)
    assert row is not None
    assert row["op_positive"] is True
    assert not np.isnan(row["PBR"])
    assert not np.isnan(row["ROE"])
    assert not np.isnan(row["rev_growth"])
    assert not np.isnan(row["profit_growth"])
