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
        ret_parts.append(norm.pct_change(fill_method=None))
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


def compute_volume_surge(
    sector_daily: pd.DataFrame,
    median_days: int = TURNOVER_MEDIAN_DAYS,
) -> pd.DataFrame:
    """各業種の turnover_ratio = 最終日売買代金 ÷ 直近 median_days 日の中央値。降順。

    Returns 列: sector, turnover_ratio, va, daily_return_pct
    """
    rows = []
    for sector, g in sector_daily.sort_values("Date").groupby("sector"):
        va = g["va"].to_numpy(dtype="float64")
        if len(va) < 2:
            continue
        hist = va[-(median_days + 1):-1] if len(va) > median_days else va[:-1]
        med = float(np.median(hist)) if len(hist) else np.nan
        ratio = float(va[-1] / med) if med and med > 0 else np.nan
        rows.append({
            "sector": sector,
            "turnover_ratio": ratio,
            "va": float(va[-1]),
            "daily_return_pct": float(g["ret"].iloc[-1] * 100.0),
        })
    out = pd.DataFrame(rows).sort_values("turnover_ratio", ascending=False)
    return out.reset_index(drop=True)


def _cum_return(ret_series: pd.Series, days: int) -> float:
    tail = ret_series.tail(days)
    return float((1.0 + tail).prod() - 1.0)


def compute_freshness(
    sector_daily: pd.DataFrame,
    week_days: int = WEEK_DAYS,
    month_days: int = MONTH_DAYS,
) -> pd.DataFrame:
    """週/月の累積リターンからランクを取り、rank_delta と3分類を返す。

    category: rising(週良・月悪) / winning(両方良) / falling(週悪・月良) / neutral
    Returns 列: sector, week_return, month_return, week_rank, month_rank,
                rank_delta, category
    """
    rows = []
    for sector, g in sector_daily.sort_values("Date").groupby("sector"):
        rows.append({
            "sector": sector,
            "week_return": _cum_return(g["ret"], week_days),
            "month_return": _cum_return(g["ret"], month_days),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # ランク（1 = 最良）。降順リターン → method='min'
    out["week_rank"] = out["week_return"].rank(ascending=False, method="min")
    out["month_rank"] = out["month_return"].rank(ascending=False, method="min")
    out["rank_delta"] = out["week_rank"] - out["month_rank"]

    n = len(out)
    top = max(1, int(round(n * 0.25)))  # 上位25%を「良」とする自前分位点

    def _classify(r):
        week_good = r["week_rank"] <= top
        month_good = r["month_rank"] <= top
        if week_good and month_good:
            return "winning"
        if week_good and not month_good:
            return "rising"
        if not week_good and month_good:
            return "falling"
        return "neutral"

    out["category"] = out.apply(_classify, axis=1)
    return out.sort_values("week_rank").reset_index(drop=True)


def _minmax_0_100(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    if hi <= lo:
        return pd.Series(50.0, index=s.index)
    return (s - lo) / (hi - lo) * 100.0


def compute_fund_flow(
    sector_daily: pd.DataFrame,
    median_days: int = TURNOVER_MEDIAN_DAYS,
) -> pd.DataFrame:
    """資金流入5成分を業種内相対で正規化し合成スコア(0-100)を返す。

    成分: turnover_pace / persistence / flow_return / breadth / up_turnover_share
    ※ up_turnover_share は sector_daily に上昇売買代金列が無い場合 up_ratio で代替。
    Returns 列: sector, score, turnover_pace, persistence, flow_return,
                breadth, up_turnover_share
    """
    rows = []
    for sector, g in sector_daily.sort_values("Date").groupby("sector"):
        va = g["va"].to_numpy(dtype="float64")
        hist = va[-(median_days + 1):-1] if len(va) > median_days else va[:-1]
        avg = float(np.mean(hist)) if len(hist) else np.nan
        pace = float(va[-1] / avg) if avg and avg > 0 else 1.0
        # 持続日数: 直近から連続で平均を上回った日数
        persistence = 0
        if avg and avg > 0:
            for v in va[::-1]:
                if v > avg:
                    persistence += 1
                else:
                    break
        rows.append({
            "sector": sector,
            "turnover_pace": pace,
            "persistence": float(persistence),
            "flow_return": float(g["ret"].iloc[-1]),
            "breadth": float(g["up_ratio"].iloc[-1]),
            "up_turnover_share": float(g["up_ratio"].iloc[-1]),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    comp = pd.DataFrame({
        "c_pace": _minmax_0_100(out["turnover_pace"]),
        "c_persist": _minmax_0_100(out["persistence"]),
        "c_return": _minmax_0_100(out["flow_return"]),
        "c_breadth": _minmax_0_100(out["breadth"]),
        "c_share": _minmax_0_100(out["up_turnover_share"]),
    })
    out["score"] = comp.mean(axis=1)
    return out.sort_values("score", ascending=False).reset_index(drop=True)


def compute_theme_temperature(sector_summary: pd.DataFrame) -> float:
    """市況の体温計(0-100)。上昇業種比率と中央breadthの平均。

    sector_summary 列: sector, period_return, breadth
    """
    if sector_summary.empty:
        return 50.0
    advancing = float((sector_summary["period_return"] > 0).mean()) * 100.0
    med_breadth = float(sector_summary["breadth"].median()) * 100.0
    return round((advancing + med_breadth) / 2.0, 1)


def compute_stock_signals(stock_cache: pd.DataFrame) -> pd.DataFrame:
    """stock_cache から銘柄レベル需給シグナルを組み立てる。

    Returns 列: code, code_4, company_name, sector, close, RSI,
                ma25_dev_pct, from_52w_high_pct, from_52w_low_pct, new_high
    """
    df = stock_cache.copy()
    close = pd.to_numeric(df.get("close"), errors="coerce")
    ma25 = pd.to_numeric(df.get("MA25"), errors="coerce")
    df["ma25_dev_pct"] = (close - ma25) / ma25 * 100.0
    df["from_52w_high_pct"] = pd.to_numeric(df.get("sepa_from_high"), errors="coerce")
    df["from_52w_low_pct"] = pd.to_numeric(df.get("sepa_from_low"), errors="coerce")
    df["new_high"] = df.get("mom_new_high", False).astype("boolean").fillna(False)
    cols = ["code", "code_4", "company_name", "sector", "close", "RSI",
            "ma25_dev_pct", "from_52w_high_pct", "from_52w_low_pct", "new_high"]
    return df[[c for c in cols if c in df.columns]].reset_index(drop=True)


def _with_self_rank(df: pd.DataFrame, by: str) -> pd.DataFrame:
    df = df.copy()
    df["self_rank"] = (
        df.groupby("sector")[by].rank(ascending=False, method="min").astype(int)
    )
    df["self_rank_total"] = df.groupby("sector")["sector"].transform("size")
    return df


def compute_rankings(stock_cache: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """5種のランキングを返す。各行に業種内順位 self_rank / self_rank_total 付き。

    keys: momentum, volume_surge（値上がり/値下がりは close/MA25 と将来の日次騰落で拡張）
    """
    df = stock_cache.copy()
    df["score"] = pd.to_numeric(df.get("score"), errors="coerce")
    df["vol_ratio"] = (
        pd.to_numeric(df.get("latest_volume"), errors="coerce")
        / pd.to_numeric(df.get("avg_volume"), errors="coerce")
    )
    rankings: Dict[str, pd.DataFrame] = {}
    rankings["momentum"] = _with_self_rank(
        df.sort_values("score", ascending=False), by="score"
    ).reset_index(drop=True)
    rankings["volume_surge"] = _with_self_rank(
        df.sort_values("vol_ratio", ascending=False), by="vol_ratio"
    ).reset_index(drop=True)
    return rankings
