# -*- coding: utf-8 -*-
"""
バックテスト・診断の先行リターン計算（分割対応・上場廃止対応）

backtest_value_service / diagnose_value_service / diagnose_growth_service で共有する。

【2026-09-17】旧実装（各サービスに重複）は、先行期間の終わり（fwd_target）の 14 日前より前に
取引が止まった銘柄を has_fwd_data=False として集計から外していた。つまり保有中に上場廃止した
銘柄が成績から消えていた。しかもその大半は TOB・MBO の買付プレミアムでプラスに終わっている
（2021-10〜2025-09 の月末起点で 250 日以内に上場廃止した 2,360 件: 平均 +18.3%・プラス 67%、
 PBR 1 倍以下に限ると中央値 +22.7%・プラス 77%。破綻などで -50% 以下は 2.5%）。
→ 保有中に取引が止まり、その後も価格データ自体は続いている銘柄を「上場廃止」とみなし、
  最後の取引価格で手放したとして成績に含める。

分割の扱い（従来どおり）:
  price_fwd = 先行日の生 C ÷（as_of < 日付 <= 先行日 の AdjFactor 累積積）= as_of 時点のスケール
"""
from typing import Iterable, Optional

import numpy as np
import pandas as pd

VALID_WINDOW_DAYS = 14   # 先行日が fwd_target のこの日数以内なら「期間を満了した」とみなす


def _ensure_datetime(prices_df: pd.DataFrame) -> pd.DataFrame:
    if pd.api.types.is_datetime64_any_dtype(prices_df["Date"]):
        return prices_df
    p = prices_df.copy()
    p["Date"] = pd.to_datetime(p["Date"], errors="coerce")
    return p


def calc_fwd_prices(prices_df: pd.DataFrame, as_of, forward_days: int,
                    codes: Optional[Iterable[str]] = None) -> pd.DataFrame:
    """forward_days 後（上場廃止ならその最終取引日）の価格を as_of のスケールで返す。

    Returns: DataFrame[code, fwd_date, price_fwd, delisted]
      delisted: as_of より後に取引があり、fwd_target の 14 日前より前に取引が止まり、
                かつ価格データ全体はその後も 14 日以上続いている銘柄
    """
    as_of = pd.Timestamp(as_of)
    fwd_target = as_of + pd.Timedelta(days=forward_days)
    window = pd.Timedelta(days=VALID_WINDOW_DAYS)

    p = _ensure_datetime(prices_df)
    data_end = p["Date"].max()
    if codes is not None:
        p = p[p["Code"].isin(set(codes))]

    rows = []
    for code, grp in p.groupby("Code"):
        grp = grp.sort_values("Date")
        last_trade = grp["Date"].iloc[-1]
        upto = grp[grp["Date"] <= fwd_target]
        close = pd.to_numeric(upto["C"], errors="coerce")
        upto = upto[close.notna()]
        if upto.empty:
            continue
        last_row = upto.iloc[-1]
        fwd_date = last_row["Date"]
        adj = pd.to_numeric(upto["AdjFactor"], errors="coerce").fillna(1.0)
        in_window = adj[upto["Date"] > as_of]
        sf = float(in_window.prod()) if len(in_window) > 0 else 1.0
        fwd_c = float(pd.to_numeric(last_row["C"], errors="coerce"))
        delisted = bool(as_of < last_trade < fwd_target - window and last_trade < data_end - window)
        rows.append({
            "code": code,
            "fwd_date": fwd_date,
            "price_fwd": fwd_c / sf if sf > 0 else np.nan,
            "delisted": delisted,
        })

    if not rows:
        return pd.DataFrame(columns=["code", "fwd_date", "price_fwd", "delisted"])
    return pd.DataFrame(rows)


def attach_fwd_returns(scored: pd.DataFrame, prices_df: pd.DataFrame, as_of, forward_days: int,
                       include_delisted: bool = True) -> pd.DataFrame:
    """scored に fwd_date / price_fwd / delisted / return_pct / has_fwd_data を付ける。

    include_delisted=False にすると旧挙動（保有中に上場廃止した銘柄を集計から外す）になる。
    比較診断用。
    """
    fwd_target = pd.Timestamp(as_of) + pd.Timedelta(days=forward_days)
    fwd = calc_fwd_prices(prices_df, as_of, forward_days, codes=scored["code"].tolist())
    out = scored.merge(fwd, on="code", how="left")
    out["delisted"] = out["delisted"].fillna(False).astype(bool)
    out["return_pct"] = (out["price_fwd"] / out["close"] - 1) * 100
    completed = out["fwd_date"] >= fwd_target - pd.Timedelta(days=VALID_WINDOW_DAYS)
    ended = completed | out["delisted"] if include_delisted else completed
    out["has_fwd_data"] = out["fwd_date"].notna() & out["return_pct"].notna() & ended
    return out
