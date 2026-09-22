# -*- coding: utf-8 -*-
"""
バリュー株 Top20 ペーパートレード: 1 営業日の処理（毎月見直し + 余裕幅）

設計書: docs/superpowers/specs/2026-09-22-value-paper-trading-design.md

D 日の日足がそろった後に呼ぶ。順序:
  1. 分割の反映  2. 前営業日に作った注文を始値で約定（売りが先）  3. 終値の更新
  4. 上場廃止の処理  5. 終値で資産評価  6. 月末（ranking が渡された日）なら翌営業日の注文を作る
単元未満株（1 株単位）で買う前提。価格はすべてその日の生の株価。
"""
import math
from dataclasses import dataclass
from typing import Optional

import pandas as pd

from services.paper_broker import PaperBroker


@dataclass(frozen=True)
class ValuePortfolioParams:
    n_hold: int = 20          # 保有銘柄数
    exit_rank: int = 40       # この順位より下に落ちたら売る（余裕幅）
    slippage: float = 0.001   # 片道 0.1%
    delist_days: int = 14     # 株価がこの暦日数以上更新されなければ上場廃止とみなし、最後の終値で売る
    lot: int = 1              # 売買単位。1 = 単元未満株（S株）、100 = 単元株
    lot_overshoot: float = 1.1  # 単元株で、始値で 1 単元が予算をこの倍率まで超えても 1 単元は買う


def _num(x) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return float("nan")


def run_value_day(broker: PaperBroker, date, bars: pd.DataFrame, ranking: Optional[pd.DataFrame],
                  p: ValuePortfolioParams) -> None:
    """bars: その日の日足（index=Code、列 O/H/L/C/AdjFactor）
    ranking: 月末の順位表（code / rank。ハードフィルタを通過した銘柄のみ）。月末以外は None
    """
    date = pd.Timestamp(date).normalize()
    if broker.last_processed is not None and date <= pd.Timestamp(broker.last_processed):
        return
    has = bars.index
    ds = date.strftime("%Y-%m-%d")

    # 1. 分割の反映
    for code in set(broker.positions) | {o["code"] for o in broker.orders}:
        if code in has:
            f = _num(bars.at[code, "AdjFactor"])
            if math.isfinite(f) and f > 0 and abs(f - 1.0) > 1e-9:
                broker.apply_split(code, f)

    # 2. 注文の約定（売りが先。売れなかった売り注文は翌営業日に持ち越し、買えなかった買い注文は取り消し）
    equity_prev = broker.last_equity()
    orders, broker.orders = broker.orders, []
    for o in [o for o in orders if o["side"] == "sell"]:
        code = o["code"]
        if code not in broker.positions:
            continue
        op = _num(bars.at[code, "O"]) if code in has else float("nan")
        if op > 0:
            broker.sell(code, op * (1 - p.slippage), date, "rank")
        else:
            broker.orders.append(o)
    target = equity_prev / p.n_hold
    for o in [o for o in orders if o["side"] == "buy"]:
        code = o["code"]
        op = _num(bars.at[code, "O"]) if code in has else float("nan")
        if code in broker.positions or not (op > 0):
            continue
        price = op * (1 + p.slippage)
        shares = int(math.floor(min(target, broker.cash) / price / p.lot)) * p.lot
        if shares == 0 and p.lot > 1 and price * p.lot <= min(broker.cash, target * p.lot_overshoot):
            shares = p.lot
        if shares >= 1:
            broker.buy(code, shares, price, date, stop=0.0, high=op)
            broker.positions[code]["last_seen"] = ds

    # 3. 終値の更新
    for code, pos in broker.positions.items():
        if code in has:
            cl = _num(bars.at[code, "C"])
            if cl > 0:
                pos["last_close"] = cl
                pos["last_seen"] = ds

    # 4. 上場廃止（株価が長く更新されない保有は最後の終値で売ったとみなす）
    for code in list(broker.positions):
        seen = broker.positions[code].get("last_seen", broker.positions[code]["entry_date"])
        if (date - pd.Timestamp(seen)).days >= p.delist_days:
            broker.sell(code, broker.positions[code]["last_close"], date, "delisted")
            broker.orders = [o for o in broker.orders if o["code"] != code]

    # 5. 資産評価
    broker.equity_curve.append({"date": ds, "equity": broker.equity(), "cash": broker.cash,
                                "n_positions": len(broker.positions)})

    # 6. 月末: 翌営業日の注文
    if ranking is not None:
        rank = dict(zip(ranking["code"].astype(str), ranking["rank"]))
        pending_sells = {o["code"] for o in broker.orders if o["side"] == "sell"}
        sells = [c for c in broker.positions if rank.get(c, math.inf) > p.exit_rank and c not in pending_sells]
        keep = len(broker.positions) - len(sells) - len(pending_sells)
        buys = [c for c in ranking.sort_values("rank")["code"].astype(str) if c not in broker.positions]
        if p.lot > 1:
            # 単元株: 月末終値で 1 単元が 1 銘柄の予算を超える銘柄は飛ばし、次の順位を買う
            budget = broker.equity() / p.n_hold
            buys = [c for c in buys if c in has and 0 < _num(bars.at[c, "C"]) * p.lot <= budget]
        buys = buys[:max(p.n_hold - keep, 0)]
        broker.orders += [{"code": c, "side": "sell", "signal_date": ds, "rank": rank.get(c)} for c in sells]
        broker.orders += [{"code": c, "side": "buy", "signal_date": ds, "rank": rank[c]} for c in buys]

    broker.last_processed = ds
