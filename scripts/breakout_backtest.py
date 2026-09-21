# -*- coding: utf-8 -*-
"""
ブレイクアウト順張り バックテスト（資金 300 万円のポートフォリオ再現）

設計書: docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md
使い方: .venv/Scripts/python.exe scripts/breakout_backtest.py                 # 前半のみ
        .venv/Scripts/python.exe scripts/breakout_backtest.py --periods first,second,all
出力:   data/breakout_backtest/summary.csv / trades_<期間>.csv / equity_<期間>.csv

期間は前半（ルール確定用）と後半（確定後に一度だけ確認）に分け、それぞれ新しい口座で回す。
周辺の値: 高値期間 {40, 55, 80} × トレーリング倍率 {2, 3, 4}（初期損切りは ATR×2 で固定）。
"""
import os
import sys
import time
from dataclasses import replace

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.breakout_engine import run_day                       # noqa: E402
from services.breakout_strategy import (                             # noqa: E402
    BreakoutParams, build_candidates, compute_indicators, earnings_block_set, market_filter,
)
from services.paper_broker import PaperBroker                       # noqa: E402
from services.split_adjust import normalize_close                   # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA = os.path.join(ROOT, "data")
OUT = os.path.join(DATA, "breakout_backtest")
CAPITAL = 3_000_000
PERIODS = {
    "first":  ("2022-02-01", "2024-12-31"),   # ルール確定用
    "second": ("2025-01-01", "2026-12-31"),   # 確定後に一度だけ確認
    "all":    ("2022-02-01", "2026-12-31"),   # 参考
}
GRID_DAYS = [40, 55, 80]
GRID_TRAIL = [2.0, 3.0, 4.0]
PRICE_COLS = ["Date", "Code", "O", "H", "L", "C", "Vo", "Va", "AdjFactor"]


def load_data():
    t = time.time()
    prices = pd.read_parquet(os.path.join(DATA, "prices.parquet"), columns=PRICE_COLS)
    prices["Date"] = pd.to_datetime(prices["Date"])
    prices["Code"] = prices["Code"].astype(str)
    prices = prices.sort_values(["Code", "Date"]).reset_index(drop=True)

    fins = pd.read_parquet(os.path.join(DATA, "fins_cache.parquet"), columns=["Code", "DocType"])
    dt = fins["DocType"].astype(str)
    stmt = dt.str.contains("FinancialStatements") & ~dt.str.contains("REIT")
    reit = set(fins.loc[dt.str.contains("REIT"), "Code"].astype(str))
    company_codes = set(fins.loc[stmt, "Code"].astype(str)) - reit

    mktcap = pd.read_parquet(os.path.join(DATA, "valuation.parquet"), columns=["Date", "Code", "MktCap"])
    earnings = pd.read_parquet(os.path.join(DATA, "earnings_dates.parquet"))
    print(f"データ読み込み {time.time() - t:.0f} 秒: 株価 {len(prices):,} 行 / 会社 {len(company_codes):,}")
    return prices, company_codes, mktcap, earnings


def all_indicators(prices: pd.DataFrame, p: BreakoutParams) -> pd.DataFrame:
    t = time.time()
    parts = [compute_indicators(g, p) for _, g in prices.groupby("Code", sort=False)]
    ind = pd.concat(parts, ignore_index=True)
    print(f"  指標（高値 {p.breakout_days} 日）{time.time() - t:.0f} 秒")
    return ind


def build_bars(ind: pd.DataFrame, start) -> dict:
    cols = ["Code", "O", "H", "L", "C", "AdjFactor", "atr_raw"]
    sub = ind[ind["Date"] >= pd.Timestamp(start)]
    return {d: g[cols].set_index("Code") for d, g in sub.groupby("Date")}


def simulate(p: BreakoutParams, dates, bars: dict, cands_by_date: dict, start, end):
    broker = PaperBroker(CAPITAL)
    empty = pd.DataFrame(columns=["Code", "vol_ratio", "atr_raw", "C"])
    ds = [d for d in dates if pd.Timestamp(start) <= d <= pd.Timestamp(end)]
    for d in ds:
        run_day(broker, d, bars.get(d, empty.set_index("Code")), cands_by_date.get(d, empty), p)
    # 期末の保有は期末終値（片道スリッページ込み）で評価して取引に含める
    last = ds[-1]
    open_trades = []
    for code, pos in broker.positions.items():
        px = pos["last_close"] * (1 - p.slippage)
        open_trades.append({
            "code": code, "entry_date": pos["entry_date"], "exit_date": last.strftime("%Y-%m-%d"),
            "shares": pos["shares"], "entry_price": pos["entry_price"], "exit_price": px,
            "pnl": pos["shares"] * (px - pos["entry_price"]), "pnl_pct": px / pos["entry_price"] - 1,
            "holding_days": int(np.busday_count(pos["entry_date"], last.strftime("%Y-%m-%d"))),
            "reason": "open_at_end",
        })
    trades = pd.DataFrame(broker.trades + open_trades)
    equity = pd.DataFrame(broker.equity_curve)
    return trades, equity


def topix_cagr(prices: pd.DataFrame, start, end) -> float:
    cp = prices[prices["Code"] == "13060"].sort_values("Date").reset_index(drop=True)
    cp = cp[pd.to_numeric(cp["C"], errors="coerce").notna()].reset_index(drop=True)
    nc = normalize_close(cp, dropna=False)
    s = pd.Series(nc.values, index=cp["Date"])
    s = s[(s.index >= pd.Timestamp(start)) & (s.index <= pd.Timestamp(end))]
    years = (s.index[-1] - s.index[0]).days / 365.25
    return (s.iloc[-1] / s.iloc[0]) ** (1 / years) - 1


def stats(trades: pd.DataFrame, equity: pd.DataFrame) -> dict:
    eq = equity.set_index(pd.to_datetime(equity["date"]))["equity"]
    years = (eq.index[-1] - eq.index[0]).days / 365.25
    cagr = (eq.iloc[-1] / CAPITAL) ** (1 / years) - 1
    mdd = (eq / eq.cummax() - 1).min()
    n = len(trades)
    win = trades.loc[trades["pnl"] > 0, "pnl"].sum() if n else 0.0
    loss = -trades.loc[trades["pnl"] < 0, "pnl"].sum() if n else 0.0
    exposure = ((equity["equity"] - equity["cash"]) / equity["equity"]).mean()
    months = years * 12
    return {
        "cagr": cagr, "final_equity": eq.iloc[-1], "max_dd": mdd,
        "trades": n, "trades_per_month": n / months if months else np.nan,
        "profit_factor": win / loss if loss > 0 else np.inf,
        "win_rate": (trades["pnl"] > 0).mean() if n else np.nan,
        "avg_win_pct": trades.loc[trades["pnl"] > 0, "pnl_pct"].mean() if n else np.nan,
        "avg_loss_pct": trades.loc[trades["pnl"] <= 0, "pnl_pct"].mean() if n else np.nan,
        "avg_hold_days": trades["holding_days"].mean() if n else np.nan,
        "exposure": exposure,
    }


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--periods", default="first", help="first / second / all をカンマ区切り")
    periods = {k: PERIODS[k] for k in ap.parse_args().periods.split(",")}
    os.makedirs(OUT, exist_ok=True)
    prices, company_codes, mktcap, earnings = load_data()
    base = BreakoutParams()

    dates = sorted(prices.loc[prices["Code"] == base.market_code, "Date"].unique())
    dates = [pd.Timestamp(d) for d in dates]
    market_ok = market_filter(prices[prices["Code"] == base.market_code], base)
    t = time.time()
    earn_block = earnings_block_set(earnings, dates, base.earnings_buffer_days)
    print(f"  決算前の除外 {len(earn_block):,} 件（{time.time() - t:.0f} 秒）")

    rows = []
    bars = None
    for days in GRID_DAYS:
        pd_ = replace(base, breakout_days=days)
        ind = all_indicators(prices, pd_)
        if bars is None:
            bars = build_bars(ind, min(v[0] for v in periods.values()))
        cands = build_candidates(ind, mktcap, company_codes, market_ok, earn_block, pd_)
        cands_by_date = {d: g for d, g in cands.groupby("Date")}
        print(f"  買い候補 {len(cands):,} 件（高値 {days} 日）")
        del ind
        for trail in GRID_TRAIL:
            p = replace(pd_, trail_atr=trail)
            for name, (s, e) in periods.items():
                trades, equity = simulate(p, dates, bars, cands_by_date, s, e)
                st = stats(trades, equity)
                st.update({"period": name, "breakout_days": days, "trail_atr": trail,
                           "topix_cagr": topix_cagr(prices, equity["date"].iloc[0], equity["date"].iloc[-1]),
                           "start": equity["date"].iloc[0], "end": equity["date"].iloc[-1]})
                rows.append(st)
                if days == base.breakout_days and trail == base.trail_atr:
                    trades.to_csv(os.path.join(OUT, f"trades_{name}.csv"), index=False, encoding="utf-8-sig")
                    equity.to_csv(os.path.join(OUT, f"equity_{name}.csv"), index=False, encoding="utf-8-sig")
            print(f"    トレーリング {trail}: 完了")

    summary = pd.DataFrame(rows)
    summary.to_csv(os.path.join(OUT, f"summary_{'_'.join(periods)}.csv"), index=False, encoding="utf-8-sig")
    report(summary, base)


def report(summary: pd.DataFrame, base: BreakoutParams):
    pd.set_option("display.width", 200)
    fmt = summary.copy()
    for c in ["cagr", "topix_cagr", "max_dd", "win_rate", "avg_win_pct", "avg_loss_pct", "exposure"]:
        fmt[c] = (fmt[c] * 100).round(1)
    for c in ["profit_factor", "trades_per_month", "avg_hold_days"]:
        fmt[c] = fmt[c].round(2)
    cols = ["period", "breakout_days", "trail_atr", "cagr", "topix_cagr", "profit_factor", "max_dd",
            "trades", "trades_per_month", "win_rate", "avg_win_pct", "avg_loss_pct", "avg_hold_days", "exposure"]
    print(fmt[cols].sort_values(["period", "breakout_days", "trail_atr"]).to_string(index=False))

    print("\n合格判定（初期値: 高値 55 日・トレーリング ATR×3）")
    for name in [n for n in ["first", "second"] if n in set(summary["period"])]:
        b = summary[(summary["period"] == name) & (summary["breakout_days"] == base.breakout_days)
                    & (summary["trail_atr"] == base.trail_atr)].iloc[0]
        nb = summary[summary["period"] == name]
        checks = {
            "年率 > TOPIX": b["cagr"] > b["topix_cagr"],
            "PF >= 1.3": b["profit_factor"] >= 1.3,
            "最大DD <= 20%": b["max_dd"] >= -0.20,
            "取引数 >= 50": b["trades"] >= 50,
            "周辺の値で PF >= 1.1": (nb["profit_factor"] >= 1.1).all(),
        }
        mark = " / ".join(f"{k}: {'OK' if v else 'NG'}" for k, v in checks.items())
        print(f"  {name}: {mark}")


if __name__ == "__main__":
    main()
