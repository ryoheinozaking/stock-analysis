# -*- coding: utf-8 -*-
"""
ペーパー口座（仮想資金の現金・保有・注文・約定の記録）

設計書: docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md
実運用に移るときは、同じ窓口（buy / sell / positions / orders / cash）を持つ証券会社版に差し替える。
価格はすべて「その日の生の株価」。分割は apply_split で保有と注文を換算する。
"""
import json
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd


class PaperBroker:
    def __init__(self, cash: float):
        self.initial_cash = float(cash)
        self.cash = float(cash)
        self.positions: Dict[str, dict] = {}     # code → shares / entry_price / entry_date / stop / highest / last_close
        self.orders: List[dict] = []              # 翌営業日の寄付きで約定させる買い注文（優先順）
        self.trades: List[dict] = []              # 決済済みの取引
        self.equity_curve: List[dict] = []        # date / equity / cash / n_positions
        self.last_processed: Optional[str] = None

    # ── 売買 ────────────────────────────────────────────────────────────
    def buy(self, code: str, shares: int, price: float, date, stop: float, high: float) -> None:
        cost = shares * price
        if cost > self.cash + 1e-6:
            raise ValueError(f"現金不足: {code} {shares}株 × {price:.1f} > {self.cash:.0f}")
        self.cash -= cost
        self.positions[code] = {
            "shares": int(shares), "entry_price": float(price),
            "entry_date": pd.Timestamp(date).strftime("%Y-%m-%d"),
            "stop": float(stop), "highest": float(high), "last_close": float(price),
        }

    def sell(self, code: str, price: float, date, reason: str) -> dict:
        pos = self.positions.pop(code)
        self.cash += pos["shares"] * price
        d_in, d_out = pos["entry_date"], pd.Timestamp(date).strftime("%Y-%m-%d")
        trade = {
            "code": code, "entry_date": d_in, "exit_date": d_out, "shares": pos["shares"],
            "entry_price": pos["entry_price"], "exit_price": float(price),
            "pnl": pos["shares"] * (price - pos["entry_price"]),
            "pnl_pct": price / pos["entry_price"] - 1.0,
            "holding_days": int(np.busday_count(d_in, d_out)),
            "reason": reason,
        }
        self.trades.append(trade)
        return trade

    def apply_split(self, code: str, factor: float) -> None:
        """その日に効力が発生した分割（AdjFactor = f、1:2 分割なら 0.5）で保有と注文を換算する。"""
        if code in self.positions:
            pos = self.positions[code]
            pos["shares"] = int(round(pos["shares"] / factor))
            for k in ("entry_price", "stop", "highest", "last_close"):
                pos[k] *= factor
        for o in self.orders:
            if o["code"] == code:
                o["shares"] = int(round(o["shares"] / factor))
                o["atr"] *= factor

    # ── 評価 ────────────────────────────────────────────────────────────
    def equity(self) -> float:
        """現金 + 保有の直近終値評価。"""
        return self.cash + sum(p["shares"] * p["last_close"] for p in self.positions.values())

    def last_equity(self) -> float:
        return self.equity_curve[-1]["equity"] if self.equity_curve else self.equity()

    # ── 保存 ────────────────────────────────────────────────────────────
    def to_dict(self) -> dict:
        return {
            "initial_cash": self.initial_cash, "cash": self.cash, "positions": self.positions,
            "orders": self.orders, "trades": self.trades, "equity_curve": self.equity_curve,
            "last_processed": self.last_processed,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PaperBroker":
        b = cls(d["initial_cash"])
        b.cash = d["cash"]
        b.positions = d["positions"]
        b.orders = d["orders"]
        b.trades = d["trades"]
        b.equity_curve = d["equity_curve"]
        b.last_processed = d["last_processed"]
        return b

    def save(self, path) -> None:
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=1), encoding="utf-8")
        tmp.replace(path)

    @classmethod
    def load(cls, path) -> "PaperBroker":
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
