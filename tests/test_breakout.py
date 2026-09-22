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


# ════════════════════════════════════════════════════════════════════════
#  Task 2 / 3: ペーパー口座と 1 営業日の処理
# ════════════════════════════════════════════════════════════════════════

from services.breakout_engine import run_day            # noqa: E402
from services.paper_broker import PaperBroker           # noqa: E402

_P = BreakoutParams()


def _bars(rows):
    """{code: (O, H, L, C, AdjFactor, atr_raw)} → run_day に渡す DataFrame。"""
    cols = ["Code", "O", "H", "L", "C", "AdjFactor", "atr_raw"]
    return pd.DataFrame([[k, *v] for k, v in rows.items()], columns=cols).set_index("Code")


def _cands(date, rows):
    return pd.DataFrame([dict(Date=_TS(date), Code=k, vol_ratio=v[0], atr_raw=v[1], C=v[2])
                         for k, v in rows.items()])


def _holding(stop=900.0, shares=100, entry=1000.0):
    b = PaperBroker(3_000_000)
    b.buy("11110", shares, entry, _TS("2024-01-04"), stop=stop, high=entry)
    b.last_processed = "2024-01-04"
    return b


def test_order_size_from_risk():
    """資産 300 万・ATR 50 → 損切り幅 100 円 → 15,000 ÷ 100 = 150 株 → 100 株単位で 100 株。"""
    b = PaperBroker(3_000_000)
    run_day(b, _TS("2024-01-04"), _bars({}), _cands("2024-01-04", {"11110": (3.0, 50.0, 1000.0)}), _P)
    assert [(o["code"], o["shares"]) for o in b.orders] == [("11110", 100)]


def test_order_size_capped_at_20pct():
    """ATR が小さく株数が大きくなっても、金額は資産の 20%（60 万円）まで。"""
    b = PaperBroker(3_000_000)
    run_day(b, _TS("2024-01-04"), _bars({}), _cands("2024-01-04", {"11110": (3.0, 1.0, 1000.0)}), _P)
    assert b.orders[0]["shares"] == 600


def test_order_skipped_when_one_lot_exceeds_cap():
    b = PaperBroker(3_000_000)
    run_day(b, _TS("2024-01-04"), _bars({}), _cands("2024-01-04", {"11110": (3.0, 100.0, 7000.0)}), _P)
    assert b.orders == []


def test_fill_at_next_open_with_slippage_and_initial_stop():
    b = PaperBroker(3_000_000)
    run_day(b, _TS("2024-01-04"), _bars({}), _cands("2024-01-04", {"11110": (3.0, 50.0, 1000.0)}), _P)
    run_day(b, _TS("2024-01-05"), _bars({"11110": (1010, 1030, 1005, 1020, 1.0, 50.0)}), _cands("2024-01-05", {}), _P)
    pos = b.positions["11110"]
    assert pos["entry_price"] == pytest.approx(1010 * 1.001)
    # 初期損切り = 始値 − ATR×2 = 910。高値 1030 − ATR×3 = 880 は低いので引き上げない
    assert pos["stop"] == pytest.approx(910.0)
    assert b.cash == pytest.approx(3_000_000 - 100 * 1010 * 1.001)


def test_order_cancelled_when_no_open():
    b = PaperBroker(3_000_000)
    run_day(b, _TS("2024-01-04"), _bars({}), _cands("2024-01-04", {"11110": (3.0, 50.0, 1000.0)}), _P)
    run_day(b, _TS("2024-01-05"), _bars({}), _cands("2024-01-05", {}), _P)
    assert b.positions == {} and b.orders == []


def test_fill_limited_by_cash():
    b = PaperBroker(150_000)
    b.orders = [{"code": "11110", "shares": 300, "atr": 50.0, "signal_date": "2024-01-04"}]
    b.last_processed = "2024-01-04"
    run_day(b, _TS("2024-01-05"), _bars({"11110": (1000, 1000, 1000, 1000, 1.0, 50.0)}), _cands("2024-01-05", {}), _P)
    # 20% 上限 = 3 万円で 100 株未満 → 見送り（資産 15 万円の 20%）
    assert b.positions == {}


def test_stop_intraday_fills_at_stop():
    b = _holding(stop=900.0)
    run_day(b, _TS("2024-01-05"), _bars({"11110": (950, 960, 890, 940, 1.0, 20.0)}), _cands("2024-01-05", {}), _P)
    assert b.positions == {}
    assert b.trades[0]["exit_price"] == pytest.approx(900 * 0.999)
    assert b.trades[0]["reason"] == "stop"


def test_stop_gap_down_fills_at_open():
    b = _holding(stop=900.0)
    run_day(b, _TS("2024-01-05"), _bars({"11110": (850, 870, 840, 860, 1.0, 20.0)}), _cands("2024-01-05", {}), _P)
    assert b.trades[0]["exit_price"] == pytest.approx(850 * 0.999)
    assert b.trades[0]["pnl"] == pytest.approx(100 * (850 * 0.999 - 1000))


def test_trailing_stop_only_rises():
    b = _holding(stop=900.0)
    run_day(b, _TS("2024-01-05"), _bars({"11110": (1000, 1200, 1000, 1150, 1.0, 20.0)}), _cands("2024-01-05", {}), _P)
    assert b.positions["11110"]["stop"] == pytest.approx(1200 - 60)
    # ATR が広がっても損切りラインは下げない
    run_day(b, _TS("2024-01-09"), _bars({"11110": (1150, 1160, 1145, 1150, 1.0, 80.0)}), _cands("2024-01-09", {}), _P)
    assert b.positions["11110"]["stop"] == pytest.approx(1140)


def test_split_rescales_position():
    b = _holding(stop=900.0, shares=100, entry=1000.0)
    run_day(b, _TS("2024-01-05"), _bars({"11110": (505, 510, 500, 505, 0.5, 10.0)}), _cands("2024-01-05", {}), _P)
    pos = b.positions["11110"]
    assert pos["shares"] == 200
    assert pos["entry_price"] == pytest.approx(500.0)
    # 損切り 900 → 450 に換算（安値 500 で掛からない）→ 引け後に 高値 510 − ATR 10×3 = 480 へ引き上げ
    assert pos["stop"] == pytest.approx(480.0)


def test_same_day_is_not_processed_twice():
    b = _holding(stop=900.0)
    bars = _bars({"11110": (950, 960, 890, 940, 1.0, 20.0)})
    run_day(b, _TS("2024-01-04"), bars, _cands("2024-01-04", {}), _P)   # 処理済みの日
    assert "11110" in b.positions and b.trades == []


def test_no_new_orders_when_not_allowed():
    b = PaperBroker(3_000_000)
    run_day(b, _TS("2024-01-04"), _bars({}), _cands("2024-01-04", {"11110": (3.0, 50.0, 1000.0)}), _P,
            allow_new=False)
    assert b.orders == []


def test_broker_roundtrip(tmp_path):
    b = _holding()
    b.orders = [{"code": "22220", "shares": 100, "atr": 5.0, "signal_date": "2024-01-04"}]
    path = tmp_path / "state.json"
    b.save(path)
    b2 = PaperBroker.load(path)
    assert b2.to_dict() == b.to_dict()


# ════════════════════════════════════════════════════════════════════════
#  押し目買い（2026-09-22 追加）: 反発判定・保有上限・上方修正・割安
# ════════════════════════════════════════════════════════════════════════

from services.breakout_strategy import (                  # noqa: E402
    pbr_lower_half_set, revision_events, revision_window_set,
)


def _uptrend_pullback_rebound(n=300, rebound=True):
    """上昇トレンド → 50 日線まで 5 日下落 → 最終日に前日高値を上抜けて反発。"""
    dates = pd.bdate_range("2023-01-02", periods=n)
    c = 1000 * 1.002 ** np.arange(n)
    c = c.copy()
    base = c[n - 7]
    for i, k in enumerate(range(n - 6, n - 1)):          # 5 日かけて約 −5%（50 日線の少し下）
        c[k] = base * (1 - 0.010 * (i + 1))
    c[n - 1] = c[n - 2] * (1.03 if rebound else 0.995)
    df = pd.DataFrame({"Date": dates, "Code": "99990", "O": c, "H": c * 1.005, "L": c * 0.995,
                       "C": c, "Vo": 100_000.0, "AdjFactor": 1.0})
    df["Va"] = df["C"] * df["Vo"]
    return df


def test_pullback_signal_fires_on_rebound():
    ind = compute_indicators(_uptrend_pullback_rebound(), BreakoutParams())
    assert bool(ind["pullback_signal"].iloc[-1]) is True
    assert not ind["pullback_signal"].iloc[-30:-1].any()


def test_pullback_signal_needs_rebound():
    ind = compute_indicators(_uptrend_pullback_rebound(rebound=False), BreakoutParams())
    assert bool(ind["pullback_signal"].iloc[-1]) is False


def test_time_exit_at_close():
    p = BreakoutParams(max_hold_days=3)
    b = _holding(stop=900.0)                                     # 2024-01-04（木）買付
    for d in ["2024-01-05", "2024-01-08"]:
        run_day(b, _TS(d), _bars({"11110": (1000, 1010, 995, 1005, 1.0, 20.0)}), _cands(d, {}), p)
    assert "11110" in b.positions
    run_day(b, _TS("2024-01-09"), _bars({"11110": (1000, 1010, 995, 1008, 1.0, 20.0)}), _cands("2024-01-09", {}), p)
    assert b.positions == {}
    assert b.trades[0]["reason"] == "time"
    assert b.trades[0]["exit_price"] == pytest.approx(1008 * 0.999)


def test_revision_events_same_fy_and_threshold():
    f = pd.DataFrame({
        "Code":     ["11110"] * 4 + ["22220"] * 2,
        "DiscDate": ["2023-05-10", "2023-08-10", "2023-09-01", "2023-11-10", "2023-08-10", "2023-09-01"],
        "DocType":  ["FYFinancialStatements_Consolidated_JP", "1QFinancialStatements_Consolidated_JP",
                     "EarnForecastRevision", "2QFinancialStatements_Consolidated_JP",
                     "1QFinancialStatements_Consolidated_JP", "EarnForecastRevision"],
        "CurFYEn":  ["2023-03-31", "2024-03-31", "2024-03-31", "2024-03-31", "2024-03-31", "2024-03-31"],
        "FNP":      [999.0, 100.0, 125.0, 130.0, 100.0, 110.0],
    })
    ev = revision_events(f, threshold=0.20)
    # 11110: 100 → 125（+25%）が上方修正。125 → 130 は +4% で対象外。本決算（旧年度）は比較に使わない
    # 22220: +10% で対象外
    assert list(zip(ev["Code"], ev["DiscDate"].dt.strftime("%Y-%m-%d"))) == [("11110", "2023-09-01")]


def test_revision_events_ignore_loss_base():
    f = pd.DataFrame({
        "Code": ["11110", "11110"], "DiscDate": ["2023-08-10", "2023-09-01"],
        "DocType": ["1QFinancialStatements_Consolidated_JP", "EarnForecastRevision"],
        "CurFYEn": ["2024-03-31", "2024-03-31"], "FNP": [-100.0, 50.0],
    })
    assert revision_events(f, threshold=0.20).empty


def test_revision_window_set():
    ev = pd.DataFrame({"Code": ["11110"], "DiscDate": [_TS("2023-09-01")]})
    td = pd.bdate_range("2023-08-01", "2023-12-29")
    s = revision_window_set(ev, td, window_days=60)
    assert ("11110", _TS("2023-09-01")) in s
    assert ("11110", _TS("2023-10-31")) in s
    assert ("11110", _TS("2023-11-01")) not in s
    assert ("11110", _TS("2023-08-31")) not in s


def test_pbr_lower_half_set():
    u = pd.DataFrame({"Date": [_TS("2024-01-04")] * 4, "Code": ["a", "b", "c", "d"],
                      "PBR": [0.5, 1.0, 2.0, np.nan]})
    s = pbr_lower_half_set(u)
    assert s == {("a", _TS("2024-01-04")), ("b", _TS("2024-01-04"))}
