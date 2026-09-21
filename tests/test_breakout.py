# -*- coding: utf-8 -*-
"""
ブレイクアウト順張り（ペーパートレード）の回帰テスト（2026-09-22）

設計書: docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md
"""
import numpy as np
import pandas as pd
import pytest

from services.breakout_strategy import (
    BreakoutParams, compute_indicators, earnings_block_set, market_filter,
)

_TS = pd.Timestamp


# ════════════════════════════════════════════════════════════════════════
#  合成データヘルパー
# ════════════════════════════════════════════════════════════════════════

def _uptrend_then_breakout(n=300, vol_mult=3.0, code="99990"):
    """緩やかな上昇（1 日 +0.1%）の後、最終日に高値を 2% 上抜け + 出来高 vol_mult 倍。"""
    dates = pd.bdate_range("2023-01-02", periods=n)
    c = 1000 * 1.001 ** np.arange(n)
    df = pd.DataFrame({"Date": dates, "Code": code, "O": c, "H": c * 1.01, "L": c * 0.99,
                       "C": c, "Vo": 100_000.0, "AdjFactor": 1.0})
    last_high = df["H"].iloc[-56:-1].max()
    df.loc[n - 1, ["O", "C"]] = last_high * 1.02
    df.loc[n - 1, "H"] = last_high * 1.03
    df.loc[n - 1, "Vo"] = 100_000.0 * vol_mult
    df["Va"] = df["C"] * df["Vo"]
    return df


# ════════════════════════════════════════════════════════════════════════
#  Task 1: 指標と判定
# ════════════════════════════════════════════════════════════════════════

def test_tech_signal_fires_on_volume_breakout():
    ind = compute_indicators(_uptrend_then_breakout(), BreakoutParams())
    assert bool(ind["tech_signal"].iloc[-1]) is True
    assert not ind["tech_signal"].iloc[:-1].any()


def test_tech_signal_needs_volume():
    ind = compute_indicators(_uptrend_then_breakout(vol_mult=1.2), BreakoutParams())
    assert bool(ind["tech_signal"].iloc[-1]) is False


def test_tech_signal_needs_uptrend():
    cp = _uptrend_then_breakout()
    # 前半を高く・後半を低くして 200 日線を下向きにする
    cp.loc[:150, ["O", "H", "L", "C"]] *= 3.0
    ind = compute_indicators(cp, BreakoutParams())
    assert bool(ind["tech_signal"].iloc[-1]) is False


def test_atr_raw_is_on_that_days_scale():
    """1:2 分割の前日は、分割前の生の値幅で ATR を返す（正規化値の 2 倍）。"""
    cp = _uptrend_then_breakout()
    k = 250
    cp.loc[k:, ["O", "H", "L", "C"]] /= 2.0
    cp.loc[k, "AdjFactor"] = 0.5
    ind = compute_indicators(cp, BreakoutParams())
    before, after = ind["atr_raw"].iloc[k - 1], ind["atr_raw"].iloc[k + 25]
    # 値幅は株価の約 2%。分割前の株価は分割後の約 2 倍
    assert before == pytest.approx(cp["C"].iloc[k - 1] * 0.02, rel=0.1)
    assert after == pytest.approx(cp["C"].iloc[k + 25] * 0.02, rel=0.1)
    # 分割を挟んでも 55 日高値のブレイクと誤判定しない（正規化系列で判定している）
    assert not ind["tech_signal"].iloc[k:-1].any()


def test_turnover_avg_is_20day_mean():
    ind = compute_indicators(_uptrend_then_breakout(), BreakoutParams())
    cp = _uptrend_then_breakout()
    assert ind["turnover_avg"].iloc[-2] == pytest.approx(cp["Va"].iloc[-21:-1].mean())


def test_market_filter_above_ma200():
    dates = pd.bdate_range("2023-01-02", periods=260)
    c = np.r_[np.full(230, 1000.0), np.full(30, 1100.0)]
    cp = pd.DataFrame({"Date": dates, "Code": "13060", "O": c, "H": c, "L": c, "C": c,
                       "Vo": 1.0, "Va": 1.0, "AdjFactor": 1.0})
    ok = market_filter(cp, BreakoutParams())
    assert bool(ok.iloc[-1]) is True
    assert not ok.iloc[:199].any()          # 200 日線ができるまでは買わない


def test_earnings_block_uses_latest_published_schedule():
    td = pd.bdate_range("2023-09-01", "2023-11-30")
    e = pd.DataFrame({
        "Code":    ["11110", "11110"],
        "FQName":  ["2Q", "2Q"],
        "FYE":     ["2024-03-31", "2024-03-31"],
        "PubDate": ["2023-09-01", "2023-09-20"],
        "SchDate": ["2023-10-10", "2023-10-30"],     # 9/20 に 10/30 へ延期
    })
    block = earnings_block_set(e, td, buffer_days=3)
    # 延期前の公表時点: 9/15 は 10/10 の 3 営業日前ではないので止めない
    assert ("11110", _TS("2023-09-15")) not in block
    # 延期後: 10/5（旧予定 10/10 の 3 営業日前）は止めない
    assert ("11110", _TS("2023-10-05")) not in block
    # 10/30 の 3 営業日前（10/25）〜前日（10/27）は止める。当日と翌日以降は止めない
    for d in ["2023-10-25", "2023-10-26", "2023-10-27"]:
        assert ("11110", _TS(d)) in block
    assert ("11110", _TS("2023-10-24")) not in block
    assert ("11110", _TS("2023-10-30")) not in block


def test_earnings_block_before_first_publication_is_empty():
    td = pd.bdate_range("2023-09-01", "2023-11-30")
    e = pd.DataFrame({"Code": ["11110"], "FQName": ["2Q"], "FYE": ["2024-03-31"],
                      "PubDate": ["2023-10-06"], "SchDate": ["2023-10-10"]})
    block = earnings_block_set(e, td, buffer_days=3)
    # 10/5 時点では予定がまだ公表されていない（先読みしない）
    assert ("11110", _TS("2023-10-05")) not in block
    assert ("11110", _TS("2023-10-06")) in block
