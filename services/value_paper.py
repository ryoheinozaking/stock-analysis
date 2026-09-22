# -*- coding: utf-8 -*-
"""
バリュー株 Top10 ペーパー運用（Top10・単元未満株・20 位より下で売る。2026-09-22 決定）

設計書: docs/superpowers/specs/2026-09-22-value-paper-trading-design.md

「データ更新」の後に run_update() を呼ぶと、前回処理した日の翌営業日から最新日までを 1 日ずつ処理する。
月末の最終営業日の大引け時点の順位で翌営業日の注文を作る（バックテストと同じ関数・同じ順序）。
祝日の暦は持たないので、月末は株価データの日付から判定する（month_end_triggers）。
"""
import os
from typing import Callable, Dict, Iterable, List, Optional, Set

import pandas as pd

from services.paper_broker import PaperBroker
from services.value_portfolio import ValuePortfolioParams, run_value_day

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAPER_DIR = os.path.join(_ROOT, "data", "paper")
STATE_PATH = os.path.join(PAPER_DIR, "value_top10.json")
RANK_DIR = os.path.join(PAPER_DIR, "rankings")
CAPITAL = 3_000_000
PARAMS = ValuePortfolioParams(n_hold=10, exit_rank=20, lot=1)


def _maybe_holiday(d: pd.Timestamp) -> bool:
    """月末に来うる休場日（年末の 12/31、4/29 が日曜の年の振替休日 4/30）。"""
    if d.month == 12 and d.day == 31:
        return True
    return d.month == 4 and d.day == 30 and pd.Timestamp(d.year, 4, 29).dayofweek == 6


def _remaining_weekdays(d: pd.Timestamp) -> List[pd.Timestamp]:
    end = d + pd.offsets.MonthEnd(0)
    return list(pd.bdate_range(d + pd.Timedelta(days=1), end))


def month_end_triggers(dates: Iterable) -> Set[pd.Timestamp]:
    """各月のデータ最終日のうち、月末の最終営業日と確定できる日。

    確定の条件: 翌月以降のデータがある / その月に残る平日が無い。
    残る平日が「休場かもしれない日」だけなら、翌月のデータが来るまで判断しない。
    """
    ds = sorted(pd.Timestamp(d).normalize() for d in dates)
    out = set()
    for i, d in enumerate(ds):
        if i + 1 < len(ds) and ds[i + 1].to_period("M") == d.to_period("M"):
            continue
        if i + 1 < len(ds) or not _remaining_weekdays(d):
            out.add(d)
    return out


def _undecided(d: pd.Timestamp) -> bool:
    """データ最終日 d が月末かどうか、翌月のデータが来るまで分からない。"""
    rest = _remaining_weekdays(d)
    return bool(rest) and all(_maybe_holiday(x) for x in rest)


def advance(broker: PaperBroker, bars_by_date: Dict[pd.Timestamp, pd.DataFrame],
            rank_fn: Callable[[pd.Timestamp], pd.DataFrame],
            p: ValuePortfolioParams = PARAMS) -> List[pd.Timestamp]:
    """未処理の営業日を古い順に処理し、処理した日を返す。

    初回（last_processed が無い）は最新日だけを処理し、その日の順位で買い注文を作る（すぐ運用を始める）。
    最新日が月末かどうか未確定なら、その日は処理せず次回に回す。
    """
    dates = sorted(pd.Timestamp(d).normalize() for d in bars_by_date)
    if not dates:
        return []
    if broker.last_processed is None:
        d = dates[-1]
        run_value_day(broker, d, bars_by_date[d], rank_fn(d), p)
        return [d]
    last = pd.Timestamp(broker.last_processed)
    todo = [d for d in dates if d > last]
    if todo and _undecided(todo[-1]):
        todo = todo[:-1]
    triggers = month_end_triggers(dates)
    done = []
    for d in todo:
        ranking = rank_fn(d) if d in triggers else None
        run_value_day(broker, d, bars_by_date[d], ranking, p)
        done.append(d)
    return done


# ════════════════════════════════════════════════════════════════════════
#  実データでの実行（Streamlit から呼ぶ）
# ════════════════════════════════════════════════════════════════════════

def load_state() -> Optional[PaperBroker]:
    return PaperBroker.load(STATE_PATH) if os.path.exists(STATE_PATH) else None


def _ranking_loader():
    """月末の順位を計算する関数を返す（バックテストと同じ run_value_snapshot。計算結果は保存して再利用）。"""
    cache: dict = {}

    def load(d: pd.Timestamp) -> pd.DataFrame:
        os.makedirs(RANK_DIR, exist_ok=True)
        path = os.path.join(RANK_DIR, f"{d:%Y-%m-%d}.pkl")
        if not os.path.exists(path):
            if not cache:
                from services.pipeline_service import _load_fins_fy, _load_prices, _load_stock_cache
                prices = _load_prices()
                prices["Date"] = pd.to_datetime(prices["Date"], errors="coerce")
                val = pd.read_parquet(os.path.join(_ROOT, "data", "valuation.parquet"),
                                      columns=["Date", "Code", "PER", "FwdPER", "PBR", "ROE", "FwdROE", "MktCap"])
                val["Date"] = pd.to_datetime(val["Date"])
                cache.update(prices=prices, fins=_load_fins_fy(), meta=_load_stock_cache(), val=val)
            from services import diagnose_value_service
            r = diagnose_value_service.run_value_snapshot(
                d.strftime("%Y-%m-%d"), cache["prices"], cache["fins"], cache["meta"], top_n=PARAMS.n_hold,
                forward_days=1, valuation=cache["val"], per_basis="forward")
            if "scored" not in r:
                raise RuntimeError(f"{d:%Y-%m-%d} の順位を計算できません: {r.get('error')}")
            cols = [c for c in ["code", "rank", "company_name", "sector", "close", "PBR", "PER", "psr",
                                "total_score"] if c in r["scored"].columns]
            pd.to_pickle(r["scored"][cols], path)
        rk = pd.read_pickle(path)
        rk["code"] = rk["code"].astype(str)
        return rk

    return load


def run_update(progress: Optional[Callable[[str], None]] = None) -> dict:
    """前回の続きから最新日まで処理して保存する。戻り値は画面表示用の要約。"""
    say = progress or (lambda m: None)
    broker = load_state() or PaperBroker(CAPITAL)
    start = None if broker.last_processed is None else pd.Timestamp(broker.last_processed)

    prices = pd.read_parquet(os.path.join(_ROOT, "data", "prices.parquet"),
                             columns=["Date", "Code", "O", "H", "L", "C", "AdjFactor"])
    prices["Date"] = pd.to_datetime(prices["Date"])
    prices["Code"] = prices["Code"].astype(str)
    latest = prices["Date"].max()
    # 月末判定に前回処理日の前後の日付も要るので、前回処理日の 10 日前から読む
    lo = latest if start is None else start - pd.Timedelta(days=10)
    sub = prices[(prices["Date"] >= lo) & prices["C"].notna()]
    bars = {d: g.set_index("Code")[["O", "H", "L", "C", "AdjFactor"]] for d, g in sub.groupby("Date")}

    say("ペーパー運用: 処理中...")
    done = advance(broker, bars, _ranking_loader())
    broker.save(STATE_PATH)
    return {"processed": [d.strftime("%Y-%m-%d") for d in done], "last_processed": broker.last_processed,
            "latest_data": latest.strftime("%Y-%m-%d"), "equity": broker.last_equity(),
            "n_positions": len(broker.positions), "n_orders": len(broker.orders)}
