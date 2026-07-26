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
