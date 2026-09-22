# -*- coding: utf-8 -*-
"""
バリュー株 Top20 ペーパートレードのバックテスト（資金 300 万円・単元未満株）

設計書: docs/superpowers/specs/2026-09-22-value-paper-trading-design.md
使い方: .venv/Scripts/python.exe scripts/value_portfolio_backtest.py
出力:   data/value_portfolio_backtest/summary.csv / trades_<型>_<期間>.csv / equity_<型>_<期間>.csv

型:
  main    毎月見直し + 余裕幅（本命）。周辺の値は売る順位 {30, 40, 60}
  tranche 12 分割（毎月その月の Top20 に 1/12 を投じて 12 か月保有）。検証済みの形の再現
毎月の順位は diagnose_valuation_source の保存済み結果（JPX 値・PER は会社予想ベース）を使い、
足りない月（2026 年分）は同じ関数で計算して data/value_portfolio_backtest/cache/ に保存する。
"""
import glob
import math
import os
import sys
import time
from dataclasses import replace

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.breakout_backtest import CAPITAL, _fmt, _judge, stats, topix_cagr   # noqa: E402
from services.paper_broker import PaperBroker                                   # noqa: E402
from services.value_portfolio import ValuePortfolioParams, run_value_day         # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
OUT = os.path.join(DATA, "value_portfolio_backtest")
DIAG_CACHE = os.path.join(DATA, "diagnose_value", "valuation_source", "cache", "fwd250_top20", "jpx_fwd")
FIRST_SNAPSHOT = "2022-06-30"
PERIODS = {
    "first":  ("2022-06-30", "2024-12-30"),   # 最初の入れ替え判断日から
    "second": ("2024-12-30", "2026-12-31"),
    "all":    ("2022-06-30", "2026-12-31"),
}
GRID_EXIT = [30, 40, 60]
BASE_EXIT = 40
PRICE_COLS = ["Date", "Code", "O", "H", "L", "C", "AdjFactor"]


def month_ends(last_date: pd.Timestamp):
    return [d.strftime("%Y-%m-%d") for d in pd.date_range(FIRST_SNAPSHOT, last_date, freq="ME")]


def load_rankings(snaps) -> dict:
    """{月末日: DataFrame(code, rank)}。保存済みが無い月は計算して保存する。"""
    own_cache = os.path.join(OUT, "cache")
    os.makedirs(own_cache, exist_ok=True)
    have = {os.path.basename(f)[:-4]: f for f in glob.glob(os.path.join(DIAG_CACHE, "*.pkl"))}
    have.update({os.path.basename(f)[:-4]: f for f in glob.glob(os.path.join(own_cache, "*.pkl"))})
    missing = [s for s in snaps if s not in have]
    if missing:
        from services import diagnose_value_service
        from services.pipeline_service import _load_fins_fy, _load_prices, _load_stock_cache
        print(f"  順位の計算: {len(missing)} か月（{missing[0]} 〜 {missing[-1]}）", flush=True)
        prices = _load_prices()
        prices["Date"] = pd.to_datetime(prices["Date"], errors="coerce")
        fins_fy, meta = _load_fins_fy(), _load_stock_cache()
        val = pd.read_parquet(os.path.join(DATA, "valuation.parquet"),
                              columns=["Date", "Code", "PER", "FwdPER", "PBR", "ROE", "FwdROE", "MktCap"])
        val["Date"] = pd.to_datetime(val["Date"])
        for s in missing:
            t = time.time()
            r = diagnose_value_service.run_value_snapshot(s, prices, fins_fy, meta, top_n=20, forward_days=250,
                                                          valuation=val, per_basis="forward")
            path = os.path.join(own_cache, f"{s}.pkl")
            pd.to_pickle(r, path)
            have[s] = path
            print(f"    {s}: 通過 {r.get('n_filtered')}（{time.time() - t:.0f} 秒）", flush=True)
    out = {}
    for s in snaps:
        r = pd.read_pickle(have[s])
        if "scored" in r:
            out[s] = r["scored"][["code", "rank"]].assign(code=lambda d: d["code"].astype(str))
    return out


def load_bars(start):
    prices = pd.read_parquet(os.path.join(DATA, "prices.parquet"), columns=PRICE_COLS)
    prices["Date"] = pd.to_datetime(prices["Date"])
    prices["Code"] = prices["Code"].astype(str)
    sub = prices[(prices["Date"] >= pd.Timestamp(start)) & prices["C"].notna()]
    bars = {d: g.set_index("Code")[["O", "H", "L", "C", "AdjFactor"]] for d, g in sub.groupby("Date")}
    return prices, bars


def trigger_days(dates, rankings: dict) -> dict:
    """月末日 → その日以前の最終営業日（順位を判断し、翌営業日に約定させる日）。"""
    idx = pd.DatetimeIndex(dates)
    out = {}
    for s, rk in rankings.items():
        i = idx.searchsorted(pd.Timestamp(s), side="right") - 1
        if i >= 0:
            out[idx[i]] = rk
    return out


def finish(broker: PaperBroker, last) -> tuple:
    open_trades = []
    for key, pos in broker.positions.items():
        px = pos["last_close"] * 0.999
        open_trades.append({
            "code": key.split("|")[0], "entry_date": pos["entry_date"], "exit_date": last.strftime("%Y-%m-%d"),
            "shares": pos["shares"], "entry_price": pos["entry_price"], "exit_price": px,
            "pnl": pos["shares"] * (px - pos["entry_price"]), "pnl_pct": px / pos["entry_price"] - 1,
            "holding_days": len(pd.bdate_range(pos["entry_date"], last)) - 1, "reason": "open_at_end",
        })
    return pd.DataFrame(broker.trades + open_trades), pd.DataFrame(broker.equity_curve)


def simulate_main(p: ValuePortfolioParams, dates, bars, triggers, start, end):
    broker = PaperBroker(CAPITAL)
    empty = pd.DataFrame(columns=["O", "H", "L", "C", "AdjFactor"])
    ds = [d for d in dates if pd.Timestamp(start) <= d <= pd.Timestamp(end)]
    for d in ds:
        run_value_day(broker, d, bars.get(d, empty), triggers.get(d), p)
    return finish(broker, ds[-1])


def simulate_tranche(dates, bars, triggers, start, end, n_hold=20, months=12, slip=0.001, delist_days=14):
    """12 分割: 毎月、12 か月前に買った分を売り、その代金（最初の 12 か月は資金の 1/12）で今月の Top20 を均等に買う。
    保有は "コード|分割番号" で管理する。"""
    broker = PaperBroker(CAPITAL)
    ds = [d for d in dates if pd.Timestamp(start) <= d <= pd.Timestamp(end)]
    pending = None          # (分割番号, 買う銘柄)
    n_trig = 0
    for d in ds:
        b = bars.get(d)
        dstr = d.strftime("%Y-%m-%d")
        if b is not None:
            for key in list(broker.positions):
                code = key.split("|")[0]
                if code in b.index and abs(b.at[code, "AdjFactor"] - 1.0) > 1e-9:
                    broker.apply_split(key, b.at[code, "AdjFactor"])
        if pending is not None and b is not None:
            k, buys = pending
            proceeds = 0.0
            for key in [x for x in broker.positions if x.endswith(f"|{k}")]:
                code = key.split("|")[0]
                op = b.at[code, "O"] if code in b.index else float("nan")
                if op > 0:
                    px = op * (1 - slip)
                    proceeds += broker.positions[key]["shares"] * px
                    broker.sell(key, px, d, "tranche")
            budget = min(broker.cash, proceeds if n_trig > months else CAPITAL / months)
            each = budget / n_hold
            for code in buys:
                op = b.at[code, "O"] if code in b.index else float("nan")
                if not (op > 0):
                    continue
                px = op * (1 + slip)
                sh = int(math.floor(min(each, broker.cash) / px))
                if sh >= 1:
                    broker.buy(f"{code}|{k}", sh, px, d, stop=0.0, high=op)
                    broker.positions[f"{code}|{k}"]["last_seen"] = dstr
            pending = None
        if b is not None:
            for key, pos in broker.positions.items():
                code = key.split("|")[0]
                if code in b.index and b.at[code, "C"] > 0:
                    pos["last_close"] = b.at[code, "C"]
                    pos["last_seen"] = dstr
        for key in list(broker.positions):
            pos = broker.positions[key]
            if (d - pd.Timestamp(pos.get("last_seen", pos["entry_date"]))).days >= delist_days:
                broker.sell(key, pos["last_close"], d, "delisted")
        broker.equity_curve.append({"date": dstr, "equity": broker.equity(), "cash": broker.cash,
                                    "n_positions": len(broker.positions)})
        if d in triggers:
            n_trig += 1
            rk = triggers[d].sort_values("rank")
            pending = (n_trig % months, list(rk["code"].head(n_hold)))
    return finish(broker, ds[-1])


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    os.makedirs(OUT, exist_ok=True)
    t0 = time.time()
    prices, bars = load_bars(FIRST_SNAPSHOT)
    dates = sorted(bars)
    snaps = month_ends(dates[-1])
    rankings = load_rankings(snaps)
    triggers = trigger_days(dates, rankings)
    print(f"読み込み {time.time() - t0:.0f} 秒: 月末の順位 {len(rankings)} か月 / 営業日 {len(dates)}", flush=True)
    if "--lots" in sys.argv:
        return compare_lots(prices, dates, bars, triggers)

    rows = []
    for name, (s, e) in PERIODS.items():
        for ex in GRID_EXIT:
            trades, equity = simulate_main(ValuePortfolioParams(exit_rank=ex), dates, bars, triggers, s, e)
            rows.append(_row(trades, equity, prices, "main", name, ex))
            if ex == BASE_EXIT:
                _save(trades, equity, "main", name)
        trades, equity = simulate_tranche(dates, bars, triggers, s, e)
        rows.append(_row(trades, equity, prices, "tranche", name, None))
        _save(trades, equity, "tranche", name)
        print(f"  {name}: 完了（{time.time() - t0:.0f} 秒）", flush=True)

    summary = pd.DataFrame(rows)
    summary.to_csv(os.path.join(OUT, "summary.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 220)
    cols = ["variant", "period", "exit_rank", "cagr", "topix_cagr", "profit_factor", "max_dd", "trades",
            "trades_per_month", "win_rate", "avg_win_pct", "avg_loss_pct", "avg_hold_days", "exposure"]
    print(_fmt(summary)[cols].to_string(index=False))
    print("\n合格判定（本命: 売る順位 40）")
    for name in ["first", "second"]:
        nb = summary[(summary["variant"] == "main") & (summary["period"] == name)]
        b = nb[nb["exit_rank"] == BASE_EXIT].iloc[0]
        print(f"  {name}: " + " / ".join(f"{k}: {'OK' if ok else 'NG'}" for k, ok in _judge(b, nb).items()))


# 銘柄数と売買単位の比較（--lots）: (名前, 保有数, 売買単位, 売る順位)
LOT_VARIANTS = [
    ("top20_s",      20, 1,   40),
    ("top10_s",      10, 1,   20),
    ("top10_s_x40",  10, 1,   40),
    ("top10_100",    10, 100, 20),
    ("top10_100_x40", 10, 100, 40),
]


def compare_lots(prices, dates, bars, triggers):
    rows = []
    for name, (s, e) in PERIODS.items():
        for v, n, lot, ex in LOT_VARIANTS:
            trades, equity = simulate_main(ValuePortfolioParams(n_hold=n, lot=lot, exit_rank=ex),
                                           dates, bars, triggers, s, e)
            rows.append(_row(trades, equity, prices, v, name, ex))
            _save(trades, equity, v, name)
    summary = pd.DataFrame(rows)
    summary.to_csv(os.path.join(OUT, "summary_lots.csv"), index=False, encoding="utf-8-sig")
    pd.set_option("display.width", 220)
    cols = ["variant", "period", "exit_rank", "cagr", "topix_cagr", "profit_factor", "max_dd", "trades",
            "win_rate", "avg_hold_days", "exposure"]
    print(_fmt(summary)[cols].to_string(index=False))


def _row(trades, equity, prices, variant, period, exit_rank):
    st = stats(trades, equity)
    st.update({"variant": variant, "period": period, "exit_rank": exit_rank,
               "topix_cagr": topix_cagr(prices, equity["date"].iloc[0], equity["date"].iloc[-1]),
               "start": equity["date"].iloc[0], "end": equity["date"].iloc[-1]})
    return st


def _save(trades, equity, variant, period):
    trades.to_csv(os.path.join(OUT, f"trades_{variant}_{period}.csv"), index=False, encoding="utf-8-sig")
    equity.to_csv(os.path.join(OUT, f"equity_{variant}_{period}.csv"), index=False, encoding="utf-8-sig")


if __name__ == "__main__":
    main()
