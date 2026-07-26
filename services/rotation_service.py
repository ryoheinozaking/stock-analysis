# -*- coding: utf-8 -*-
"""セクター回転・需給ランキングの純粋計算（Streamlit 非依存）。

KabuTrend /trend を参考 UI としつつ、指標定義・閾値は自前データで決める。
分割スケール破綻を避けるため、リターンは split_adjust.normalize_close 経由で算出する。
"""
from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from services import split_adjust

# ── パラメータ（後でバックテストで調整可能なよう外出し） ──
DEFAULT_WINDOW_DAYS = 90
MIN_STOCKS_PER_SECTOR = 3
WEEK_DAYS = 5
MONTH_DAYS = 20
TURNOVER_MEDIAN_DAYS = 20


def load_sector_daily(
    prices: pd.DataFrame,
    sector_map: Dict[str, str],
    window_days: int = DEFAULT_WINDOW_DAYS,
    min_stocks: int = MIN_STOCKS_PER_SECTOR,
) -> pd.DataFrame:
    """各 (Date, sector) の等加重リターン・売買代金合計・銘柄数・上昇比率を返す。

    Returns 列: Date, sector, ret, va, n, up_ratio
    """
    df = prices[["Date", "Code", "C", "Va", "AdjFactor"]].copy()
    # 直近 window_days 営業日に絞る
    dates = sorted(df["Date"].unique())[-window_days:]
    df = df[df["Date"].isin(dates)]
    df = df.sort_values(["Code", "Date"])

    # 銘柄ごとに分割正規化 close → pct_change
    # （groupby().apply() は単一グループ時に戻り値の形が不安定になるため、
    #   明示的にコードごとループして concat する）
    ret_parts = []
    for _code, cp in df.groupby("Code", sort=False):
        cp = cp.sort_values("Date")
        norm = split_adjust.normalize_close(cp, dropna=False)
        ret_parts.append(norm.pct_change())
    df["ret"] = pd.concat(ret_parts) if ret_parts else pd.Series(dtype=float)
    df["sector"] = df["Code"].map(sector_map)
    df = df.dropna(subset=["sector"])

    grp = df.groupby(["Date", "sector"])
    out = grp.agg(
        ret=("ret", "mean"),
        va=("Va", "sum"),
        n=("Code", "size"),
        up_ratio=("ret", lambda s: float((s > 0).mean())),
    ).reset_index()
    # 構成銘柄数が閾値未満の業種は除外
    out = out[out["n"] >= min_stocks].reset_index(drop=True)
    return out
