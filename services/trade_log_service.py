# -*- coding: utf-8 -*-
import os
import warnings
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

from services.split_adjust import (
    normalize_close, normalize_volume, normalize_high, normalize_low, cum_factor,
)

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
        cp_full = prices[prices["Code"] == code5].copy()
        if cp_full.empty:
            return None, None
        cp_full["Date"] = pd.to_datetime(cp_full["Date"], errors="coerce")
        cp_full = cp_full.sort_values("Date").reset_index(drop=True)
        # 分割対応: 全期間で正規化してから entry 日までの30日に切り出す
        close_norm = normalize_close(cp_full, dropna=False)
        vol_norm   = normalize_volume(cp_full, fillna=True)

        cutoff = pd.Timestamp(date_entry)
        mask   = cp_full["Date"] <= cutoff
        closes = close_norm[mask].dropna().tail(30).values
        vols   = vol_norm[mask].tail(30).values
        if len(closes) < 20:
            return None, None

        # RSI（14日）— スケール正規化済の close で計算
        deltas = np.diff(closes)
        gains  = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)
        avg_gain = gains[-14:].mean() if len(gains) >= 14 else gains.mean()
        avg_loss = losses[-14:].mean() if len(losses) >= 14 else losses.mean()
        rsi = 100 - (100 / (1 + avg_gain / avg_loss)) if avg_loss > 0 else 100.0

        # 出来高比率（当日 / 20日平均）— 株数ベースに正規化済
        latest_vol = vols[-1] if len(vols) >= 1 else 0
        avg_vol    = vols[-20:].mean() if len(vols) >= 20 else vols.mean() if len(vols) > 0 else 0
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


def _calc_exit_metrics(
    ticker: str,
    date_entry: str,
    date_exit: str,
    entry_price: float,
    exit_price: float,
):
    """
    エグジット時の統計を計算する。
    Returns: (pnl_pct, holding_days, max_profit_pct, max_loss_pct)
    """
    # I-5: ゼロ除算ガード
    if entry_price == 0:
        raise ValueError("entry_price が 0 です")
    pnl_pct      = (exit_price - entry_price) / entry_price * 100
    date_e = pd.Timestamp(date_entry)
    date_x = pd.Timestamp(date_exit)
    holding_days = (date_x - date_e).days
    if holding_days < 0:
        raise ValueError(f"date_exit ({date_exit}) は date_entry ({date_entry}) より前です")

    # MFE/MAE: prices.parquet から保有期間の高値・安値を取得
    # 分割対応: 保有期間中に分割があり得るので、entry/exit/H/L すべてを
    # 「entry-day スケール」に揃えてから比較する。
    max_profit_pct = pnl_pct  # fallback
    max_loss_pct   = pnl_pct  # fallback
    try:
        prices = pd.read_parquet(PRICES_PATH)
        code5  = ticker + "0"
        cp_full = prices[prices["Code"] == code5].copy()
        cp_full["Date"] = pd.to_datetime(cp_full["Date"], errors="coerce")
        cp_full = cp_full.sort_values("Date").reset_index(drop=True)
        if not cp_full.empty:
            # 末尾日スケールで正規化した H/L
            high_norm = normalize_high(cp_full, dropna=False)
            low_norm  = normalize_low(cp_full,  dropna=False)
            cum = cum_factor(cp_full)
            # entry_price は "entry 日のスケール" → 末尾日スケールへ変換
            ts_e = pd.Timestamp(date_entry)
            # entry 日に最も近い行（同日 or 直前）の cum_factor を取得
            idx_e_arr = cp_full.index[cp_full["Date"] <= ts_e]
            if len(idx_e_arr) > 0:
                idx_e = idx_e_arr[-1]
                cum_e = float(cum.iloc[idx_e])
                entry_price_norm = entry_price * cum_e
                # 保有期間内の H/L（末尾日スケール）
                ts_x = pd.Timestamp(date_exit)
                hold_mask = (cp_full["Date"] >= ts_e) & (cp_full["Date"] <= ts_x)
                if hold_mask.any() and entry_price_norm > 0:
                    max_high = float(high_norm[hold_mask].max())
                    min_low  = float(low_norm[hold_mask].min())
                    max_profit_pct = (max_high - entry_price_norm) / entry_price_norm * 100
                    max_loss_pct   = (min_low  - entry_price_norm) / entry_price_norm * 100
    except Exception:
        pass

    return pnl_pct, holding_days, max_profit_pct, max_loss_pct


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
    df.at[i, "max_profit_pct"] = round(max_profit_pct, 2)
    df.at[i, "max_loss_pct"]   = round(max_loss_pct, 2)
    save(df)
    return df
