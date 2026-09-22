# -*- coding: utf-8 -*-
"""
ブレイクアウト順張り: 1 営業日の処理（バックテストとペーパー運用の共通処理）

設計書: docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md

D 日の日足がそろった後に呼ぶ。順序:
  1. 分割の反映  2. 前日注文を始値で約定  3. 損切り判定（3b. 保有上限で大引け売り）  4. トレーリング引き上げ
  5. 終値で資産評価  6. D 日の引けの候補から翌営業日の注文を作る
"""
import math

import numpy as np
import pandas as pd

from services.breakout_strategy import BreakoutParams
from services.paper_broker import PaperBroker


def _lots(value: float, price: float, lot: int) -> int:
    if not (price > 0) or not math.isfinite(value) or value <= 0:
        return 0
    return int(math.floor(value / price / lot)) * lot


def _num(x) -> float:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return float("nan")
    return v


def run_day(broker: PaperBroker, date, bars: pd.DataFrame, candidates: pd.DataFrame,
            p: BreakoutParams, allow_new: bool = True) -> None:
    """bars: その日の日足（index=Code、列 O/H/L/C/AdjFactor/atr_raw。生の株価）
    candidates: その日の買い候補（Code / vol_ratio / atr_raw / C）
    allow_new: False ならこの日は新規注文を作らない（データが古い日など）
    """
    date = pd.Timestamp(date).normalize()
    if broker.last_processed is not None and date <= pd.Timestamp(broker.last_processed):
        return
    has = bars.index

    # 1. 分割の反映
    codes = set(broker.positions) | {o["code"] for o in broker.orders}
    for code in codes:
        if code in has:
            f = _num(bars.at[code, "AdjFactor"])
            if math.isfinite(f) and f > 0 and abs(f - 1.0) > 1e-9:
                broker.apply_split(code, f)

    # 2. 前日注文を始値で約定（始値が無い = 売買なしなら取り消し）
    equity_prev = broker.last_equity()
    orders, broker.orders = broker.orders, []
    for o in orders:
        code = o["code"]
        if code in broker.positions or code not in has:
            continue
        op = _num(bars.at[code, "O"])
        if not (op > 0):
            continue
        price = op * (1 + p.slippage)
        shares = min(int(o["shares"]),
                     _lots(p.max_pos_pct * equity_prev, price, p.lot),
                     _lots(broker.cash, price, p.lot))
        if shares < p.lot:
            continue
        broker.buy(code, shares, price, date, stop=op - p.init_stop_atr * o["atr"], high=op)

    # 3. 損切り判定（当日約定分も含む。窓を開けた下落は始値で約定）
    for code in list(broker.positions):
        if code not in has:
            continue
        pos = broker.positions[code]
        lo, op = _num(bars.at[code, "L"]), _num(bars.at[code, "O"])
        if lo <= pos["stop"]:
            px = min(op, pos["stop"]) if op > 0 else pos["stop"]
            broker.sell(code, px * (1 - p.slippage), date, "stop")

    # 3b. 保有の上限に達したら大引けで売る
    if p.max_hold_days:
        d = date.strftime("%Y-%m-%d")
        for code in list(broker.positions):
            pos = broker.positions[code]
            cl = _num(bars.at[code, "C"]) if code in has else float("nan")
            if cl > 0 and np.busday_count(pos["entry_date"], d) >= p.max_hold_days:
                broker.sell(code, cl * (1 - p.slippage), date, "time")

    # 4. トレーリング引き上げ（上げるのみ）と終値の更新
    for code, pos in broker.positions.items():
        if code not in has:
            continue
        hi, cl, atr = (_num(bars.at[code, c]) for c in ("H", "C", "atr_raw"))
        if hi > 0:
            pos["highest"] = max(pos["highest"], hi)
        if atr > 0:
            pos["stop"] = max(pos["stop"], pos["highest"] - p.trail_atr * atr)
        if cl > 0:
            pos["last_close"] = cl

    # 5. 資産評価
    eq = broker.equity()
    broker.equity_curve.append({"date": date.strftime("%Y-%m-%d"), "equity": eq,
                                "cash": broker.cash, "n_positions": len(broker.positions)})

    # 6. 翌営業日の注文
    if allow_new and candidates is not None and len(candidates):
        risk = p.risk_pct * eq
        for r in candidates.sort_values("vol_ratio", ascending=False).itertuples(index=False):
            if r.Code in broker.positions:
                continue
            atr, close = _num(r.atr_raw), _num(r.C)
            if not (atr > 0 and close > 0):
                continue
            shares = min(_lots(risk / (p.init_stop_atr * atr), 1.0, p.lot),
                         _lots(p.max_pos_pct * eq, close, p.lot))
            if shares >= p.lot:
                broker.orders.append({"code": r.Code, "shares": shares, "atr": atr,
                                      "signal_date": date.strftime("%Y-%m-%d"),
                                      "signal_close": close, "vol_ratio": _num(r.vol_ratio)})

    broker.last_processed = date.strftime("%Y-%m-%d")
