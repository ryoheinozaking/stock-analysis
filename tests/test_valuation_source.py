# -*- coding: utf-8 -*-
"""J-Quants バリュエーション指標での置き換え（services/valuation_source.py）のテスト"""
import numpy as np
import pandas as pd


def _valuation():
    return pd.DataFrame({
        "Date":   pd.to_datetime(["2026-09-15", "2026-09-16", "2026-09-16"]),
        "Code":   ["A", "A", "B"],
        "PER":    [10.0, 11.0, 30.0],
        "FwdPER": [12.0, 13.0, np.nan],
        "PBR":    [1.0, 1.1, 3.0],
        "ROE":    [0.08, 0.09, 0.20],
        "FwdROE": [0.10, 0.11, np.nan],
        "MktCap": [100.0, 110.0, 300.0],
    })


def test_forward_basis_sets_per_roe_market_cap_and_date():
    from services.valuation_source import apply_jpx_valuation

    snap = pd.DataFrame({"code": ["A", "B", "C"], "PER": [5.0] * 3, "PBR": [0.5] * 3, "ROE": [1.0] * 3})
    out = apply_jpx_valuation(snap, _valuation(), "2026-09-16", per_basis="forward").set_index("code")

    assert out.loc["A", "PER"] == 13.0
    assert out.loc["A", "ROE"] == 11.0                    # 小数 → %
    assert out.loc["A", "market_cap"] == 110.0 * 1e6
    assert out.loc["A", "valuation_date"] == "2026-09-16"
    assert np.isnan(out.loc["B", "PER"]) and np.isnan(out.loc["B", "ROE"])   # 予想なしは空欄のまま
    assert np.isnan(out.loc["C", "PBR"])                  # JPX に無い銘柄は自前値で埋めない


def test_live_valuation_keeps_self_calculated_columns():
    from services.valuation_source import apply_live_valuation

    stock = pd.DataFrame({"code": ["A"], "PER": [5.0], "PBR": [0.5], "ROE": [7.0], "close": [1000.0]})
    out = apply_live_valuation(stock, _valuation(), "2026-09-16").iloc[0]

    assert (out["PER"], out["PBR"], out["ROE"]) == (13.0, 1.1, 11.0)      # 本番は予想ベース
    assert (out["PER_self"], out["PBR_self"], out["ROE_self"]) == (5.0, 0.5, 7.0)
    assert out["close"] == 1000.0
