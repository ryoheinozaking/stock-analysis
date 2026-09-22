# -*- coding: utf-8 -*-
"""
バリュー株 Top20 ペーパートレード（毎月見直し + 余裕幅）の回帰テスト（2026-09-22）

設計書: docs/superpowers/specs/2026-09-22-value-paper-trading-design.md
"""
import pandas as pd
import pytest

from services.paper_broker import PaperBroker
from services.value_portfolio import ValuePortfolioParams, run_value_day

_TS = pd.Timestamp
_P = ValuePortfolioParams(n_hold=3, exit_rank=5)


def _bars(rows):
    """{code: (O, C)} または {code: (O, C, AdjFactor)} → 日足。"""
    recs = []
    for k, v in rows.items():
        o, c = v[0], v[1]
        f = v[2] if len(v) > 2 else 1.0
        recs.append([k, o, max(o, c), min(o, c), c, f])
    return pd.DataFrame(recs, columns=["Code", "O", "H", "L", "C", "AdjFactor"]).set_index("Code")


def _rank(codes):
    return pd.DataFrame({"code": codes, "rank": range(1, len(codes) + 1)})


def _held(b):
    return sorted(b.positions)


def test_first_rebalance_buys_top_n_equal_weight():
    b = PaperBroker(300_000)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["a", "b", "c", "d"]), _P)
    assert [o["code"] for o in b.orders] == ["a", "b", "c"]
    run_value_day(b, _TS("2024-02-01"), _bars({"a": (1000, 1000), "b": (3000, 3000), "c": (7, 7)}), None, _P)
    assert _held(b) == ["a", "b", "c"]
    # 1 銘柄 10 万円（30 万 ÷ 3）。1 株単位で切り捨て
    assert b.positions["a"]["shares"] == 99          # 100,000 ÷ 1001 = 99.9
    assert b.positions["b"]["shares"] == 33
    assert b.positions["c"]["shares"] == 14271      # 100,000 ÷ 7.007


def test_keep_within_buffer_and_sell_below_exit_rank():
    b = PaperBroker(300_000)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["a", "b", "c"]), _P)
    run_value_day(b, _TS("2024-02-01"), _bars({k: (1000, 1000) for k in "abc"}), None, _P)
    # 翌月: a は 4 位（余裕幅内 = 保有継続）、b は 6 位（売る）、c は順位表に無い（ハードフィルタ落ち = 売る）
    run_value_day(b, _TS("2024-02-29"), _bars({k: (1000, 1000) for k in "abc"}),
                  _rank(["x", "y", "z", "a", "w", "b"]), _P)
    sells = [o["code"] for o in b.orders if o["side"] == "sell"]
    buys = [o["code"] for o in b.orders if o["side"] == "buy"]
    assert sorted(sells) == ["b", "c"]
    assert buys == ["x", "y"]                       # 保有が 3 銘柄になるまで上位から
    run_value_day(b, _TS("2024-03-01"), _bars({k: (1100, 1100) for k in "abcxy"}), None, _P)
    assert _held(b) == ["a", "x", "y"]
    assert {t["code"]: t["reason"] for t in b.trades} == {"b": "rank", "c": "rank"}


def test_sell_order_waits_when_no_bar():
    b = PaperBroker(300_000)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["a"]), _P)
    run_value_day(b, _TS("2024-02-01"), _bars({"a": (1000, 1000)}), None, _P)
    run_value_day(b, _TS("2024-02-29"), _bars({"a": (1000, 1000)}), _rank(["x"]), _P)
    run_value_day(b, _TS("2024-03-01"), _bars({"x": (500, 500)}), None, _P)   # a は売買停止
    assert "a" in b.positions and any(o["code"] == "a" for o in b.orders)
    run_value_day(b, _TS("2024-03-04"), _bars({"a": (900, 900), "x": (500, 500)}), None, _P)
    assert "a" not in b.positions


def test_delisted_sold_at_last_close():
    b = PaperBroker(300_000)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["a"]), _P)
    run_value_day(b, _TS("2024-02-01"), _bars({"a": (1000, 1200)}), None, _P)
    for d in pd.bdate_range("2024-02-02", "2024-02-16"):
        run_value_day(b, d, _bars({}), None, _P)
    assert b.positions == {}
    t = b.trades[0]
    assert t["reason"] == "delisted" and t["exit_price"] == pytest.approx(1200)


def test_split_while_holding():
    b = PaperBroker(300_000)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["a"]), _P)
    run_value_day(b, _TS("2024-02-01"), _bars({"a": (1000, 1000)}), None, _P)
    shares = b.positions["a"]["shares"]
    run_value_day(b, _TS("2024-02-02"), _bars({"a": (500, 500, 0.5)}), None, _P)
    assert b.positions["a"]["shares"] == shares * 2
    assert b.equity_curve[-1]["equity"] == pytest.approx(b.equity_curve[-2]["equity"])


def test_not_processed_twice():
    b = PaperBroker(300_000)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["a"]), _P)
    run_value_day(b, _TS("2024-01-31"), _bars({}), _rank(["b"]), _P)
    assert [o["code"] for o in b.orders] == ["a"]
