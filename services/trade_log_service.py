# -*- coding: utf-8 -*-
import os
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TRADE_LOG_PATH = os.path.join(_ROOT, "data", "trade_log.csv")
PRICES_PATH    = os.path.join(_ROOT, "data", "prices.parquet")
CACHE_PATH     = os.path.join(_ROOT, "data", "stock_cache.parquet")

COLUMNS = [
    "id", "ticker", "company_name",
    "date_entry", "date_exit",
    "entry_price", "exit_price", "stop_price",
    "position_pct", "strategy_type", "rule_violation", "memo",
    "rsi_at_entry", "volume_ratio_at_entry",
    "pe_at_entry", "pb_at_entry", "revenue_growth",
    "pnl_pct", "holding_days", "max_profit_pct", "max_loss_pct",
]


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS)


def _validate_date(date_str: str) -> None:
    """date_str が日付としてパース可能か検証する。不正な場合は ValueError を送出する。"""
    try:
        pd.Timestamp(date_str)
    except Exception:
        raise ValueError(f"日付フォーマットが不正です: {date_str!r}")


def load() -> pd.DataFrame:
    """trade_log.csv を読み込む。存在しない場合は空のDataFrameを返す。"""
    if not os.path.exists(TRADE_LOG_PATH):
        return _empty_df()
    try:
        df = pd.read_csv(TRADE_LOG_PATH, dtype={"ticker": str, "id": str})
        for col in COLUMNS:
            if col not in df.columns:
                df[col] = None
        # M-3: rule_violation の CSV ラウンドトリップ対応
        if "rule_violation" in df.columns:
            df["rule_violation"] = (
                df["rule_violation"]
                .map({"True": True, "False": False, True: True, False: False})
                .fillna(False)
                .astype(bool)
            )
        return df.reindex(columns=COLUMNS)
    except Exception as e:
        warnings.warn(f"trade_log.csv 読み込み失敗: {e}", stacklevel=2)
        return _empty_df()


def save(df: pd.DataFrame) -> None:
    """DataFrame を trade_log.csv に保存する。"""
    os.makedirs(os.path.dirname(TRADE_LOG_PATH), exist_ok=True)
    df.reindex(columns=COLUMNS).to_csv(TRADE_LOG_PATH, index=False)


def _get_company_name(ticker: str) -> str:
    """stock_cache.parquet から会社名を取得する。"""
    try:
        df = pd.read_parquet(CACHE_PATH)
        row = df[df["code_4"] == ticker]
        if not row.empty:
            return str(row.iloc[0].get("company_name", ticker))
    except Exception:
        pass
    return ticker


def _get_price_metrics(ticker: str, date_entry: str):
    """
    prices.parquet からエントリー日時点の RSI・出来高比率を計算する。
    取得できない場合は (None, None) を返す。
    """
    try:
        prices = pd.read_parquet(PRICES_PATH)
        code5  = ticker + "0"
        df = prices[prices["Code"] == code5].copy()
        if df.empty:
            return None, None
        df = df.sort_values("Date")
        df = df[df["Date"] <= date_entry].tail(30)
        if len(df) < 2:
            return None, None

        # RSI（14日）
        closes = df["AdjC"].values
        deltas = np.diff(closes)
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_gain = gains[-14:].mean() if len(gains) >= 14 else gains.mean()
        avg_loss = losses[-14:].mean() if len(losses) >= 14 else losses.mean()
        rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100.0

        # 出来高比率（当日 / 20日平均）
        vols = df["AdjVo"].values
        latest_vol = vols[-1]
        avg_vol    = vols[-20:].mean() if len(vols) >= 20 else vols.mean()
        vol_ratio  = round(latest_vol / avg_vol, 2) if avg_vol > 0 else None

        return round(rsi, 1), vol_ratio
    except Exception:
        return None, None


def _get_fundamental_metrics(ticker: str):
    """
    stock_cache.parquet から PER・PBR・売上成長率を取得する。
    取得できない場合は (None, None, None) を返す。
    """
    try:
        df  = pd.read_parquet(CACHE_PATH)
        row = df[df["code_4"] == ticker]
        if row.empty:
            return None, None, None
        r = row.iloc[0]
        pe  = r.get("PER") if "PER" in r else None
        pb  = r.get("PBR") if "PBR" in r else None
        rev = r.get("rev_growth") if "rev_growth" in r else None
        return (
            float(pe)  if pe  is not None and not pd.isna(pe)  else None,
            float(pb)  if pb  is not None and not pd.isna(pb)  else None,
            float(rev) if rev is not None and not pd.isna(rev) else None,
        )
    except Exception:
        return None, None, None


def _calc_exit_metrics(ticker, date_entry, date_exit, entry_price, exit_price):
    # I-5: ゼロ除算ガード
    if entry_price == 0:
        raise ValueError("entry_price が 0 です")
    pnl_pct = (exit_price - entry_price) / entry_price * 100
    holding_days = (pd.Timestamp(date_exit) - pd.Timestamp(date_entry)).days
    return pnl_pct, holding_days, None, None  # Task 3 で実装


def add_entry(
    ticker: str,
    date_entry: str,
    entry_price: float,
    stop_price: float,
    position_pct: float,
    strategy_type: str,
    memo: str = "",
) -> pd.DataFrame:
    # I-4: 日付バリデーション
    _validate_date(date_entry)
    df = load()
    # I-3: ID生成を読みやすい形式に
    existing_ids = df["id"].dropna()
    if df.empty or existing_ids.empty:
        new_id = "1"
    else:
        new_id = str(int(existing_ids.astype(int).max()) + 1)
    company_name = _get_company_name(ticker)
    rsi, vol_ratio = _get_price_metrics(ticker, date_entry)
    pe, pb, rev_growth = _get_fundamental_metrics(ticker)
    row = {
        "id": new_id, "ticker": ticker, "company_name": company_name,
        "date_entry": date_entry, "date_exit": None,
        "entry_price": entry_price, "exit_price": None, "stop_price": stop_price,
        "position_pct": position_pct, "strategy_type": strategy_type,
        "rule_violation": False, "memo": memo,
        "rsi_at_entry": rsi, "volume_ratio_at_entry": vol_ratio,
        "pe_at_entry": pe, "pb_at_entry": pb, "revenue_growth": rev_growth,
        "pnl_pct": None, "holding_days": None, "max_profit_pct": None, "max_loss_pct": None,
    }
    df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    save(df)
    return df


def add_exit(
    trade_id: str,
    date_exit: str,
    exit_price: float,
    rule_violation: bool = False,
) -> pd.DataFrame:
    # I-4: 日付バリデーション
    _validate_date(date_exit)
    df = load()
    idx = df.index[df["id"] == trade_id]
    if len(idx) == 0:
        raise ValueError(f"id={trade_id} が見つかりません")
    i = idx[0]
    # I-1: 二重決済チェック
    existing_exit = df.at[i, "date_exit"]
    if existing_exit is not None and not (isinstance(existing_exit, float) and pd.isna(existing_exit)):
        raise ValueError(f"id={trade_id} はすでに決済済みです")
    ticker = str(df.at[i, "ticker"])
    date_entry = str(df.at[i, "date_entry"])
    entry_price = float(df.at[i, "entry_price"])
    pnl_pct, holding_days, max_profit_pct, max_loss_pct = _calc_exit_metrics(
        ticker, date_entry, date_exit, entry_price, exit_price
    )
    df.at[i, "date_exit"]      = date_exit
    df.at[i, "exit_price"]     = exit_price
    df.at[i, "rule_violation"] = rule_violation
    df.at[i, "pnl_pct"]        = round(pnl_pct, 2)
    df.at[i, "holding_days"]   = holding_days
    df.at[i, "max_profit_pct"] = max_profit_pct  # Task 3 で実装（None）
    df.at[i, "max_loss_pct"]   = max_loss_pct    # Task 3 で実装（None）
    save(df)
    return df
