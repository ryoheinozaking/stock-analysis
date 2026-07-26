# -*- coding: utf-8 -*-
"""services.rotation_service の単体テスト（合成データ）。"""
import numpy as np
import pandas as pd
import pytest

from services import rotation_service as rs


def _mk_prices(rows):
    """rows: list of (date, code, close, va, adjfactor) → prices風 DataFrame。"""
    return pd.DataFrame(
        rows, columns=["Date", "Code", "C", "Va", "AdjFactor"]
    )


def test_load_sector_daily_equal_weight_return():
    # 2業種・各2銘柄・3営業日。分割なし(AdjFactor=1)。
    # 銘柄A1: 100→110(+10%)→121(+10%), A2: 100→90(-10%)→99(+10%)
    # 業種X の day2 等加重リターン = (+10% + -10%)/2 = 0
    rows = [
        ("2026-01-05", "0001", 100.0, 1000.0, 1.0),
        ("2026-01-06", "0001", 110.0, 1200.0, 1.0),
        ("2026-01-07", "0001", 121.0, 1300.0, 1.0),
        ("2026-01-05", "0002", 100.0, 500.0, 1.0),
        ("2026-01-06", "0002", 90.0, 600.0, 1.0),
        ("2026-01-07", "0002", 99.0, 700.0, 1.0),
    ]
    prices = _mk_prices(rows)
    sector_map = {"0001": "X", "0002": "X"}
    out = rs.load_sector_daily(prices, sector_map, window_days=10, min_stocks=1)
    day2 = out[(out["Date"] == "2026-01-06") & (out["sector"] == "X")].iloc[0]
    assert day2["ret"] == pytest.approx(0.0, abs=1e-9)
    assert day2["va"] == pytest.approx(1800.0)  # 1200 + 600
    assert day2["n"] == 2
    assert day2["up_ratio"] == pytest.approx(0.5)  # 1銘柄上昇/2


def test_load_sector_daily_split_scale_safe():
    # 銘柄が day3 で 1:2 分割。AdjFactor は分割日に 0.5 が入る想定。
    # 生Cは 200→220→(分割後)120 だが正規化リターンは +10%,+9.09% になるべき。
    rows = [
        ("2026-01-05", "0001", 200.0, 1000.0, 1.0),
        ("2026-01-06", "0001", 220.0, 1000.0, 1.0),
        ("2026-01-07", "0001", 120.0, 1000.0, 0.5),
    ]
    prices = _mk_prices(rows)
    sector_map = {"0001": "X"}
    out = rs.load_sector_daily(prices, sector_map, window_days=10, min_stocks=1)
    day3 = out[(out["Date"] == "2026-01-07") & (out["sector"] == "X")].iloc[0]
    # 分割調整後の実質: 220相当→240相当なので +9.09%。生の 220→120 の -45% ではない。
    assert day3["ret"] > 0.05


def test_compute_volume_surge_ratio():
    # 業種X: 過去5日 va=100 一定 → 中央値100。最終日 va=270 → ratio 2.7。
    rows = []
    for i, d in enumerate(["01-01", "01-02", "01-03", "01-04", "01-05"]):
        va = 100.0 if d != "01-05" else 270.0
        rows.append((f"2026-{d}", "X", 0.0, va, 5, 0.5))
    sd = pd.DataFrame(rows, columns=["Date", "sector", "ret", "va", "n", "up_ratio"])
    out = rs.compute_volume_surge(sd, median_days=4)
    row = out[out["sector"] == "X"].iloc[0]
    assert row["turnover_ratio"] == pytest.approx(2.7)


def _mk_sector_series(sector, rets):
    """rets: 日次リターン list → sector_daily 風の行群。"""
    rows = []
    for i, r in enumerate(rets):
        rows.append((f"2026-02-{i+1:02d}", sector, r, 100.0, 5, 0.5))
    return rows


def test_compute_freshness_rising_vs_fading():
    # A: 直近1週だけ強い(月では平凡) → 上昇中。B: 月は強いが直近失速 → 失速。
    rets_A = [0.0] * 15 + [0.03] * 5           # 月20日中、後半5日だけ+3%
    rets_B = [0.03] * 15 + [-0.03] * 5          # 前半強く直近マイナス
    rows = _mk_sector_series("A", rets_A) + _mk_sector_series("B", rets_B)
    sd = pd.DataFrame(rows, columns=["Date", "sector", "ret", "va", "n", "up_ratio"])
    out = rs.compute_freshness(sd, week_days=5, month_days=20)
    a = out[out["sector"] == "A"].iloc[0]
    b = out[out["sector"] == "B"].iloc[0]
    # A は週ランクが月ランクより上位(=数値小) → rank_delta 負
    assert a["rank_delta"] < 0
    assert a["category"] == "rising"
    # B は週ランクが月ランクより下位 → rank_delta 正 → 失速
    assert b["rank_delta"] > 0
    assert b["category"] == "falling"


def test_compute_fund_flow_score_bounds_and_components():
    # 3業種、最終日に売買代金が急増しbreadthも高い業種が高スコアになること。
    rows = []
    for s, pace_last, up in [("A", 300.0, 0.9), ("B", 100.0, 0.5), ("C", 50.0, 0.1)]:
        for i in range(20):
            va = 100.0 if i < 19 else pace_last
            ret = 0.02 if (i == 19 and up > 0.5) else 0.0
            rows.append((f"2026-03-{i+1:02d}", s, ret, va, 10, up))
    sd = pd.DataFrame(rows, columns=["Date", "sector", "ret", "va", "n", "up_ratio"])
    out = rs.compute_fund_flow(sd)
    assert out["score"].between(0, 100).all()
    # A が最高スコア
    assert out.sort_values("score", ascending=False).iloc[0]["sector"] == "A"


def test_theme_temperature_range_and_direction():
    # 全業種プラス&高breadth → 高温。全業種マイナス&低breadth → 低温。
    hot_rows, cold_rows = [], []
    for s in ["A", "B", "C", "D"]:
        hot_rows.append((s, 0.02, 0.9))
        cold_rows.append((s, -0.02, 0.1))
    hot = pd.DataFrame(hot_rows, columns=["sector", "period_return", "breadth"])
    cold = pd.DataFrame(cold_rows, columns=["sector", "period_return", "breadth"])
    t_hot = rs.compute_theme_temperature(hot)
    t_cold = rs.compute_theme_temperature(cold)
    assert 0 <= t_cold < t_hot <= 100
    assert t_hot > 60
    assert t_cold < 40


def test_compute_stock_signals_uses_sepa_columns():
    sc = pd.DataFrame([
        {"code": "0001", "code_4": "0001", "company_name": "テストA",
         "sector": "X", "close": 110.0, "MA25": 100.0, "RSI": 55.0,
         "sepa_from_low": 40.0, "sepa_from_high": -2.0, "mom_new_high": True},
        {"code": "0002", "code_4": "0002", "company_name": "テストB",
         "sector": "X", "close": 90.0, "MA25": 100.0, "RSI": 45.0,
         "sepa_from_low": 3.0, "sepa_from_high": -30.0, "mom_new_high": False},
    ])
    out = rs.compute_stock_signals(sc)
    a = out[out["code"] == "0001"].iloc[0]
    # MA25乖離率 = (110-100)/100*100 = +10%
    assert a["ma25_dev_pct"] == pytest.approx(10.0)
    assert bool(a["new_high"]) is True
    assert a["from_52w_high_pct"] == pytest.approx(-2.0)
    assert a["from_52w_low_pct"] == pytest.approx(40.0)
