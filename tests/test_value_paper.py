# -*- coding: utf-8 -*-
"""
バリュー株 Top10 ペーパー運用（日々の追いつき処理・月末判定）の回帰テスト（2026-09-22）

設計書: docs/superpowers/specs/2026-09-22-value-paper-trading-design.md
"""
import pandas as pd

from services.paper_broker import PaperBroker
from services.value_paper import advance, month_end_triggers
from services.value_portfolio import ValuePortfolioParams

_TS = pd.Timestamp
_P = ValuePortfolioParams(n_hold=2, exit_rank=4)


def _days(*ds):
    return [_TS(d) for d in ds]


def test_month_end_when_next_month_data_exists():
    # 12/30 が年内最終営業日。次の平日 12/31 は同月だが、1/5 のデータがあるので月末と分かる
    assert month_end_triggers(_days("2024-12-27", "2024-12-30", "2025-01-06")) == {_TS("2024-12-30")}


def test_month_end_by_next_weekday_before_next_month_data():
    # 2024-05-31（金）の次の平日は 6/3 = 翌月。翌月のデータが無くても月末と分かる
    assert month_end_triggers(_days("2024-05-30", "2024-05-31")) == {_TS("2024-05-31")}


def test_not_month_end_mid_month_or_unknown():
    # 5/30 は月中。12/30 は翌月のデータが無く次の平日 12/31 が同月なので、まだ判断しない
    assert month_end_triggers(_days("2024-05-29", "2024-05-30")) == set()
    assert month_end_triggers(_days("2024-12-27", "2024-12-30")) == set()


def _bars(price=1000.0):
    return pd.DataFrame([[c, price, price, price, price, 1.0] for c in "abc"],
                        columns=["Code", "O", "H", "L", "C", "AdjFactor"]).set_index("Code")


def test_first_run_starts_immediately_on_latest_day():
    calls = []

    def rank_fn(d):
        calls.append(d)
        return pd.DataFrame({"code": ["a", "b", "c"], "rank": [1, 2, 3]})

    b = PaperBroker(300_000)
    bars = {_TS("2024-05-15"): _bars()}
    processed = advance(b, bars, rank_fn, _P)
    assert processed == [_TS("2024-05-15")]
    assert calls == [_TS("2024-05-15")]                 # 開始日は月中でも順位を出して買い注文を作る
    assert [o["code"] for o in b.orders] == ["a", "b"]


def test_catch_up_fills_and_triggers_only_at_month_end():
    calls = []

    def rank_fn(d):
        calls.append(d)
        return pd.DataFrame({"code": ["a", "b", "c"], "rank": [1, 2, 3]})

    b = PaperBroker(300_000)
    advance(b, {_TS("2024-05-15"): _bars()}, rank_fn, _P)
    bars = {d: _bars() for d in _days("2024-05-16", "2024-05-17", "2024-05-31", "2024-06-03")}
    processed = advance(b, bars, rank_fn, _P)
    assert processed == _days("2024-05-16", "2024-05-17", "2024-05-31", "2024-06-03")
    assert calls == _days("2024-05-15", "2024-05-31")
    assert sorted(b.positions) == ["a", "b"]            # 5/16 の寄付きで約定
    assert b.last_processed == "2024-06-03"


def test_already_processed_days_are_skipped():
    def rank_fn(d):
        return pd.DataFrame({"code": ["a"], "rank": [1]})

    b = PaperBroker(300_000)
    advance(b, {_TS("2024-05-15"): _bars()}, rank_fn, _P)
    assert advance(b, {_TS("2024-05-15"): _bars()}, rank_fn, _P) == []


def test_pending_month_end_is_processed_when_next_month_arrives():
    """12/30 の時点では月末か分からないので止め、1/6 のデータが来たら 12/30 を月末として処理する。"""
    calls = []

    def rank_fn(d):
        calls.append(d)
        return pd.DataFrame({"code": ["a"], "rank": [1]})

    b = PaperBroker(300_000)
    advance(b, {_TS("2024-12-26"): _bars()}, rank_fn, _P)
    assert advance(b, {_TS("2024-12-27"): _bars(), _TS("2024-12-30"): _bars()}, rank_fn, _P) == \
        _days("2024-12-27")                              # 12/30 は保留
    assert advance(b, {_TS("2024-12-30"): _bars(), _TS("2025-01-06"): _bars()}, rank_fn, _P) == \
        _days("2024-12-30", "2025-01-06")
    assert calls == _days("2024-12-26", "2024-12-30")
