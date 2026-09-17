# -*- coding: utf-8 -*-
"""
先行リターン（上場廃止対応）と、過去時点スナップショットの JPX 値置き換えのテスト（2026-09-17）
"""
import numpy as np
import pandas as pd
import pytest


def _series(code, start, end, price=100.0, adj=None):
    dates = pd.bdate_range(start, end)
    df = pd.DataFrame({"Date": dates, "Code": code, "C": price, "AdjFactor": 1.0})
    for d, f in (adj or {}).items():
        df.loc[df["Date"] == pd.Timestamp(d), "AdjFactor"] = f
    return df


def _prices():
    return pd.concat([
        # A: 期間満了。先行日の直前 2026-01-05 に 1:2 分割 → 生 C は 60（as_of スケールで 120）
        _series("A", "2025-06-02", "2026-01-02", 100.0),
        _series("A", "2026-01-05", "2026-03-31", 60.0, adj={"2026-01-05": 0.5}),
        # B: as_of 後の 2025-09-30 に TOB で上場廃止（最終価格 150）
        _series("B", "2025-06-02", "2025-09-29", 100.0),
        _series("B", "2025-09-30", "2025-09-30", 150.0),
        # D: as_of より前（2025-06-30）に上場廃止済みの残骸
        _series("D", "2025-06-02", "2025-06-30", 80.0),
        # 市場全体のデータは 2026-03-31 まで続く
        _series("Z", "2025-06-02", "2026-03-31", 10.0),
    ], ignore_index=True)


AS_OF = "2025-08-29"
FWD = 150   # fwd_target = 2026-01-26


def test_calc_fwd_prices_flags_delisting_and_adjusts_split():
    from services.forward_returns import calc_fwd_prices

    out = calc_fwd_prices(_prices(), AS_OF, FWD, codes=["A", "B", "D"]).set_index("code")

    assert out.loc["A", "price_fwd"] == pytest.approx(120.0)     # 60 ÷ 0.5
    assert not out.loc["A", "delisted"]
    assert out.loc["B", "price_fwd"] == pytest.approx(150.0)
    assert out.loc["B", "fwd_date"] == pd.Timestamp("2025-09-30")
    assert out.loc["B", "delisted"]
    assert not out.loc["D", "delisted"]                           # as_of 前に止まった銘柄は対象外


def test_attach_fwd_returns_includes_delisted_by_default():
    from services.forward_returns import attach_fwd_returns

    scored = pd.DataFrame({"code": ["A", "B", "D"], "close": [100.0, 100.0, 80.0]})
    out = attach_fwd_returns(scored, _prices(), AS_OF, FWD).set_index("code")
    assert out.loc["A", "return_pct"] == pytest.approx(20.0)
    assert out.loc["B", "return_pct"] == pytest.approx(50.0)
    assert out.loc["A", "has_fwd_data"] and out.loc["B", "has_fwd_data"]
    assert not out.loc["D", "has_fwd_data"]

    old = attach_fwd_returns(scored, _prices(), AS_OF, FWD, include_delisted=False).set_index("code")
    assert not old.loc["B", "has_fwd_data"]                       # 旧挙動: 上場廃止は集計外


def test_snapshot_excludes_stale_prices_of_delisted_stocks():
    """as_of の 14 日以上前から取引の無い銘柄は、過去時点の候補に入れない。"""
    from services.backtest_value_service import _build_atdate_snapshot

    prices = _prices()
    prices_past = prices[prices["Date"] <= pd.Timestamp(AS_OF)]
    fins = pd.DataFrame([
        {"Code": c, "DiscDate": pd.Timestamp("2025-05-10"), "DocType": "FYFinancialStatements_Consolidated_JP",
         "CurPerType": "FY", "CurFYEn": "2025-03-31", "EPS": 10.0, "BPS": 100.0, "NP": 1.0,
         "Eq": 10.0, "OP": 1.0, "Sales": 10.0}
        for c in ["A", "B", "D"]
    ])
    meta = pd.DataFrame({"code": pd.Series([], dtype=object)})
    snap = _build_atdate_snapshot(prices_past, fins, meta, pd.Timestamp(AS_OF))
    assert set(snap["code"]) == {"A", "B"}


def test_snapshot_jpx_valuation_overrides_per_pbr_market_cap():
    from services.backtest_value_service import apply_jpx_valuation

    snap = pd.DataFrame({"code": ["A", "B", "C"], "PER": [9.0, 9.0, 9.0], "PBR": [0.9, 0.9, 0.9]})
    val = pd.DataFrame({
        "Date": pd.to_datetime(["2025-08-20", "2025-08-29", "2025-08-29", "2025-09-01", "2025-08-15"]),
        "Code": ["A", "A", "B", "B", "C"],
        "PER": [11.0, 12.0, 20.0, 99.0, 5.0],
        "FwdPER": [13.0, 14.0, np.nan, 99.0, 6.0],
        "PBR": [1.1, 1.2, 2.0, 99.0, 0.5],
        "MktCap": [100.0, 200.0, 300.0, 999.0, 50.0],
    })

    fwd = apply_jpx_valuation(snap, val, pd.Timestamp(AS_OF), per_basis="forward").set_index("code")
    assert fwd.loc["A", "PER"] == 14.0 and fwd.loc["A", "PBR"] == 1.2       # as_of 以前で最新
    assert fwd.loc["A", "market_cap"] == 200.0 * 1e6                        # 百万円 → 円
    assert np.isnan(fwd.loc["B", "PER"])                                    # 予想なしは空欄（自前値で埋めない）
    assert np.isnan(fwd.loc["C", "PBR"])                                    # 7 日より古い値は使わない

    ttm = apply_jpx_valuation(snap, val, pd.Timestamp(AS_OF), per_basis="ttm").set_index("code")
    assert ttm.loc["A", "PER"] == 12.0 and ttm.loc["B", "PER"] == 20.0


def test_apply_hard_filter_uses_provided_market_cap():
    from services.pipeline_service import apply_hard_filter

    base = {"PBR": 1.0, "PER": 10.0, "rev_growth": 5.0, "op_positive": True, "sector": "", "close": 1000.0}
    snap = pd.DataFrame([dict(base, code="A", market_cap=5e9), dict(base, code="B", market_cap=np.nan)])
    metrics = pd.DataFrame({"code": ["A", "B"], "equity_ratio": [50.0, 50.0], "sh_out": [1e8, 1e8]})

    # 自前計算なら close×sh_out = 1000億で通過するが、渡された時価総額（50億 / 空欄）を使う
    assert apply_hard_filter(snap, metrics, mode="value").empty
    own = apply_hard_filter(snap.drop(columns=["market_cap"]), metrics, mode="value")
    assert set(own["code"]) == {"A", "B"}
