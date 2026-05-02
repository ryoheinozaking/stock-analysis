# -*- coding: utf-8 -*-
"""
バリュー株モード バックテスト（クロスセクション・コホート分析）

過去の特定時点 (snapshot) でバリュー株パイプラインを再現実行し、
選定された Top N の forward リターンを非選定群と比較する。

データソースは pipeline_service と共通の prices.parquet / fins_cache.parquet を使用。
各 snapshot について以下を計算：
  - そのときの全銘柄スナップショット (close/PER/PBR/ROE/etc) を構築
  - バリューモードのパイプラインを適用
  - Top N とそれ以外を分類
  - forward 期間後のリターンを計算
"""

import os
from typing import Optional, List, Dict, Callable
import numpy as np
import pandas as pd

from services.pipeline_service import (
    _load_prices, _load_fins_fy, _load_stock_cache,
    _build_fins_metrics, apply_hard_filter,
    calc_funda_score, calc_tech_scores, calc_total_score,
)
from services.split_adjust import split_factor_between

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ── デフォルト設定 ──────────────────────────────────────────────────────
DEFAULT_SNAPSHOTS    = ["2024-04-30", "2024-10-31", "2025-04-30"]
DEFAULT_TOP_N        = 50
DEFAULT_FORWARD_DAYS = 365


# ════════════════════════════════════════════════════════════════════════
#  スナップショット構築
# ════════════════════════════════════════════════════════════════════════

def _build_atdate_snapshot(
    prices_past: pd.DataFrame,
    fins_past:   pd.DataFrame,
    stock_meta:  pd.DataFrame,
    as_of:       pd.Timestamp,
) -> pd.DataFrame:
    """
    as_of 時点での stock_cache 相当 DataFrame を構築する。

    Returns: code, code_4, company_name, sector, market, close,
             PER, PBR, ROE, rev_growth, profit_growth, op_positive

    【スケール統一方針】
    PER/PBR は close と per-share 値（EPS/BPS）が同一スケールでないと意味を持たない。
    - close: 生の C を使用（その日のスケール = as_of 時点の真の取引価格）
    - EPS/BPS: 開示時点(disc_date)のスケールから as_of 時点のスケールに変換
      （disc_date と as_of の間に分割があれば split_factor を乗算）
    """
    # 直近終値（各銘柄ごとに as_of 以前で最新の C）
    # AdjC は J-Quants 取得タイミング次第でスケール混在のため使わない
    p = prices_past.sort_values(["Code", "Date"])
    latest_price = (p.groupby("Code")
                     .tail(1)[["Code", "C"]]
                     .rename(columns={"Code": "code", "C": "close"}))
    latest_price["close"] = pd.to_numeric(latest_price["close"], errors="coerce")

    # コード別グループ化（split_factor_between 計算用）
    p_indexed = p.copy()
    p_indexed["Date"]      = pd.to_datetime(p_indexed["Date"], errors="coerce")
    p_indexed["AdjFactor"] = pd.to_numeric(p_indexed["AdjFactor"], errors="coerce").fillna(1.0)
    prices_grouped = {code: grp for code, grp in p_indexed.groupby("Code")}

    # 各銘柄の最新FY+前FY決算
    rows = []
    for code, grp in fins_past.groupby("Code"):
        grp = grp.reset_index(drop=True)
        if len(grp) < 1:
            continue
        curr = grp.iloc[0]   # 最新（DiscDate DESC でソート済み）

        eps     = pd.to_numeric(curr.get("EPS"),   errors="coerce")
        bps     = pd.to_numeric(curr.get("BPS"),   errors="coerce")
        np_c    = pd.to_numeric(curr.get("NP"),    errors="coerce")
        eq      = pd.to_numeric(curr.get("Eq"),    errors="coerce")
        op_c    = pd.to_numeric(curr.get("OP"),    errors="coerce")
        sales_c = pd.to_numeric(curr.get("Sales"), errors="coerce")

        # 分割係数: disc_curr 〜 as_of の間に発生した分割の累積積
        # per-share 値（EPS/BPS）は disc_curr スケールから as_of スケールへ × sf
        cp = prices_grouped.get(code)
        disc_curr = pd.to_datetime(curr.get("DiscDate"), errors="coerce")
        sf_curr = (split_factor_between(cp, disc_curr, as_of)
                   if (cp is not None and pd.notna(disc_curr)) else 1.0)
        eps_n = eps * sf_curr if pd.notna(eps) else np.nan
        bps_n = bps * sf_curr if pd.notna(bps) else np.nan

        roe = (np_c / eq * 100) if (pd.notna(np_c) and pd.notna(eq) and eq > 0) else np.nan
        op_positive = bool(pd.notna(op_c) and op_c > 0)

        rev_g, profit_g = np.nan, np.nan
        if len(grp) >= 2:
            prev    = grp.iloc[1]
            sales_p = pd.to_numeric(prev.get("Sales"), errors="coerce")
            np_p    = pd.to_numeric(prev.get("NP"),    errors="coerce")
            # Sales/NP は会社全体の値（per-share でない）→ 分割の影響を受けない
            if pd.notna(sales_c) and pd.notna(sales_p) and sales_p > 0:
                rev_g = (sales_c - sales_p) / abs(sales_p) * 100
            if pd.notna(np_c) and pd.notna(np_p) and np_p != 0:
                profit_g = (np_c - np_p) / abs(np_p) * 100

        rows.append({
            "code":          code,
            "EPS_curr":      eps_n,
            "BPS_curr":      bps_n,
            "ROE":           roe,
            "rev_growth":    rev_g,
            "profit_growth": profit_g,
            "op_positive":   op_positive,
        })

    fund_df = pd.DataFrame(rows)
    if fund_df.empty:
        return pd.DataFrame()

    # マージ
    snap = latest_price.merge(fund_df, on="code", how="inner")
    snap["PER"] = np.where(
        snap["EPS_curr"].notna() & (snap["EPS_curr"] > 0),
        snap["close"] / snap["EPS_curr"],
        np.nan,
    )
    snap["PBR"] = np.where(
        snap["BPS_curr"].notna() & (snap["BPS_curr"] > 0),
        snap["close"] / snap["BPS_curr"],
        np.nan,
    )

    # メタ情報マージ（code_4/company_name/sector/market は時間不変として現在値を使う）
    meta_cols = [c for c in ["code", "code_4", "company_name", "sector", "market"] if c in stock_meta.columns]
    meta = stock_meta[meta_cols].drop_duplicates("code")
    snap = snap.merge(meta, on="code", how="left")
    return snap


# ════════════════════════════════════════════════════════════════════════
#  スナップショットごとのバックテスト
# ════════════════════════════════════════════════════════════════════════

def _apply_extra_filters(scored: pd.DataFrame, extra_filters: Optional[Dict]) -> pd.DataFrame:
    """
    追加除外ルールを scored DataFrame に適用する。
    ハードフィルタ通過後・Top N 選定前のレイヤーで動作する。

    extra_filters keys:
      - profit_growth_max:  float   利益成長率の上限（これ以下なら通過）
      - ma200_dev_max:      float   MA200乖離率の上限（%）
      - exclude_sectors:    List[str] 除外セクター名
    """
    if not extra_filters:
        return scored
    df = scored.copy()
    if "profit_growth_max" in extra_filters:
        thr = extra_filters["profit_growth_max"]
        df = df[df["profit_growth"].fillna(-999) <= thr]
    if "ma200_dev_max" in extra_filters:
        thr = extra_filters["ma200_dev_max"]
        df["_ma200_dev"] = df["tech_detail"].apply(
            lambda d: (d or {}).get("ma200_dev") if isinstance(d, dict) else None
        )
        # MA200乖離率がNoneの場合は通す（テクニカル算出不能銘柄を排除しないため）
        mask = df["_ma200_dev"].isna() | (df["_ma200_dev"] <= thr)
        df = df[mask].drop(columns=["_ma200_dev"])
    if "exclude_sectors" in extra_filters:
        excluded = set(extra_filters["exclude_sectors"])
        df = df[~df["sector"].isin(excluded)]
    return df.reset_index(drop=True)


def run_snapshot_backtest(
    as_of_date:    str,
    prices_df:     pd.DataFrame,
    fins_fy:       pd.DataFrame,
    stock_meta:    pd.DataFrame,
    top_n:         int = DEFAULT_TOP_N,
    forward_days:  int = DEFAULT_FORWARD_DAYS,
    progress_cb:   Optional[Callable] = None,
    extra_filters: Optional[Dict] = None,
) -> Dict:
    """1スナップショット日のバリュー株バックテスト。"""
    def _cb(m):
        if progress_cb:
            progress_cb(m)

    as_of      = pd.Timestamp(as_of_date)
    fwd_target = as_of + pd.Timedelta(days=forward_days)

    _cb(f"[{as_of_date}] 過去データ抽出中...")
    p = prices_df.copy()
    p["Date"] = pd.to_datetime(p["Date"], errors="coerce")
    prices_past = p[p["Date"] <= as_of].copy()

    fins_past = fins_fy[fins_fy["DiscDate"] <= as_of].copy()

    if prices_past.empty or fins_past.empty:
        return {"as_of": as_of_date, "error": "過去データが不足"}

    _cb(f"[{as_of_date}] スナップショット構築中...")
    snap_df = _build_atdate_snapshot(prices_past, fins_past, stock_meta, as_of)
    if snap_df.empty:
        return {"as_of": as_of_date, "error": "スナップショット構築失敗"}

    _cb(f"[{as_of_date}] ファンダ指標計算中...")
    fins_metrics = _build_fins_metrics(fins_past, prices_past)

    _cb(f"[{as_of_date}] ハードフィルタ適用中...")
    filtered = apply_hard_filter(snap_df, fins_metrics, mode="value")

    if filtered.empty:
        return {"as_of": as_of_date, "error": "フィルタ通過銘柄なし", "n_filtered": 0}

    _cb(f"[{as_of_date}] ファンダスコア計算中... ({len(filtered)} 銘柄)")
    scored = calc_funda_score(filtered, mode="value")

    _cb(f"[{as_of_date}] テクニカルスコア計算中...")
    scored = calc_tech_scores(scored, prices_past, mode="value")

    _cb(f"[{as_of_date}] 最終スコア計算中...")
    scored = calc_total_score(scored)

    if extra_filters:
        before = len(scored)
        scored = _apply_extra_filters(scored, extra_filters)
        _cb(f"[{as_of_date}] 追加除外ルール適用: {before} → {len(scored)} 銘柄")

    _cb(f"[{as_of_date}] フォワードリターン計算中...")
    # 分割対応: フォワードリターンは「as_of スケール」で close と price_fwd を統一する
    # スナップショットの close は C[as_of]（as_of-day スケール）。
    # price_fwd は C[fwd_date] × split_factor(fwd_date → as_of) で as_of スケールに戻す。
    # ※ split_factor は as_of < d <= fwd_date 区間の AdjFactor 累積積。
    #    price_fwd を as_of スケールで見るには その係数を掛ける（per-share 値と同じ）。
    p_fwd = p[p["Date"] <= fwd_target].sort_values(["Code", "Date"])

    fwd_rows = []
    for code, grp in p_fwd.groupby("Code"):
        grp = grp.sort_values("Date").reset_index(drop=True)
        if grp.empty:
            continue
        last_row = grp.iloc[-1]
        fwd_date  = last_row["Date"]
        fwd_C_raw = pd.to_numeric(last_row["C"], errors="coerce")
        # as_of < d <= fwd_date 区間の累積分割係数
        af = pd.to_numeric(grp["AdjFactor"], errors="coerce").fillna(1.0)
        af_in_window = af[(grp["Date"] > as_of) & (grp["Date"] <= fwd_date)]
        sf = float(af_in_window.prod()) if len(af_in_window) > 0 else 1.0
        # as_of スケールに戻した forward 価格
        # 分割で価格は 1/N に下がる → as_of スケールで比較するには ÷sf （= ×1/sf）
        if pd.notna(fwd_C_raw) and sf > 0:
            price_fwd_in_as_of_scale = fwd_C_raw / sf
        else:
            price_fwd_in_as_of_scale = np.nan
        fwd_rows.append({
            "code":      code,
            "fwd_date":  fwd_date,
            "price_fwd": price_fwd_in_as_of_scale,
        })
    fwd_price = pd.DataFrame(fwd_rows)

    scored = scored.merge(fwd_price, on="code", how="left")
    scored["return_pct"] = (scored["price_fwd"] / scored["close"] - 1) * 100

    # フォワード日付が target に近い銘柄のみ有効データとして扱う（早すぎる上場廃止等を排除）
    scored["has_fwd_data"] = (
        scored["fwd_date"].notna()
        & (scored["fwd_date"] >= fwd_target - pd.Timedelta(days=14))
        & scored["return_pct"].notna()
    )

    # 順位付け
    scored = scored.sort_values("total_score", ascending=False).reset_index(drop=True)
    scored["rank"]   = scored.index + 1
    scored["is_top"] = scored["rank"] <= top_n

    return {
        "as_of":      as_of_date,
        "fwd_date":   fwd_target.strftime("%Y-%m-%d"),
        "scored":     scored,
        "n_filtered": len(filtered),
    }


# ════════════════════════════════════════════════════════════════════════
#  集計
# ════════════════════════════════════════════════════════════════════════

def _stats(s: pd.Series) -> Dict:
    """リターン系列の統計サマリー。"""
    if len(s) == 0:
        return {"n": 0, "mean": None, "median": None, "win_rate": None,
                "p25": None, "p75": None, "min": None, "max": None,
                "trap_rate": None}
    return {
        "n":         int(len(s)),
        "mean":      round(float(s.mean()),   2),
        "median":    round(float(s.median()), 2),
        "win_rate":  round(float((s > 0).sum())  / len(s) * 100, 1),
        "p25":       round(float(s.quantile(0.25)), 2),
        "p75":       round(float(s.quantile(0.75)), 2),
        "min":       round(float(s.min()),    2),
        "max":       round(float(s.max()),    2),
        "trap_rate": round(float((s <= -20).sum()) / len(s) * 100, 1),  # -20%以下＝バリュートラップ
    }


def aggregate_results(snapshot_results: List[Dict]) -> Dict:
    """複数スナップショットの結果を統合集計"""
    all_top, all_rest = [], []

    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"].copy()
        df["snapshot"] = r["as_of"]
        valid = df[df["has_fwd_data"]].copy()
        all_top.append(valid[valid["is_top"]])
        all_rest.append(valid[~valid["is_top"]])

    if not all_top:
        return {"error": "集計可能なデータがありません"}

    top_df  = pd.concat(all_top,  ignore_index=True) if all_top  else pd.DataFrame()
    rest_df = pd.concat(all_rest, ignore_index=True) if all_rest else pd.DataFrame()

    top_stats  = _stats(top_df["return_pct"]) if len(top_df) else _stats(pd.Series([]))
    rest_stats = _stats(rest_df["return_pct"]) if len(rest_df) else _stats(pd.Series([]))

    diff_mean = (
        round(top_stats["mean"] - rest_stats["mean"], 2)
        if top_stats["mean"] is not None and rest_stats["mean"] is not None else None
    )

    return {
        "top":       top_stats,
        "rest":      rest_stats,
        "diff_mean": diff_mean,
        "top_df":    top_df,
        "rest_df":   rest_df,
    }


# ════════════════════════════════════════════════════════════════════════
#  メインエントリ
# ════════════════════════════════════════════════════════════════════════

def run_backtest(
    snapshots:     Optional[List[str]] = None,
    top_n:         int = DEFAULT_TOP_N,
    forward_days:  int = DEFAULT_FORWARD_DAYS,
    progress_cb:   Optional[Callable] = None,
    extra_filters: Optional[Dict] = None,
) -> Dict:
    """
    バリュー株モード バックテストのメインエントリ。

    Returns:
        {
          "snapshots":         [...],
          "top_n":             int,
          "forward_days":      int,
          "snapshot_results":  [...],
          "aggregate":         {...},
          "data_range":        (min_date, max_date),
        }
    """
    snapshots = snapshots or DEFAULT_SNAPSHOTS

    def _cb(m):
        if progress_cb:
            progress_cb(m)

    _cb("既存データ読み込み中...")
    prices_df  = _load_prices()
    fins_fy    = _load_fins_fy()
    stock_meta = _load_stock_cache()

    p_dates = pd.to_datetime(prices_df["Date"], errors="coerce")
    p_min, p_max = p_dates.min(), p_dates.max()
    _cb(f"価格データ範囲: {p_min.date()} 〜 {p_max.date()}")

    snap_results = []
    for snap_date in snapshots:
        snap_ts = pd.Timestamp(snap_date)
        if snap_ts < p_min:
            _cb(f"[{snap_date}] スキップ：価格データ開始日({p_min.date()})より前")
            snap_results.append({"as_of": snap_date, "error": f"価格データ開始日({p_min.date()})より前"})
            continue
        if snap_ts + pd.Timedelta(days=forward_days) > p_max + pd.Timedelta(days=14):
            _cb(f"[{snap_date}] 警告：フォワード期間が価格データ終端を超える可能性")
        result = run_snapshot_backtest(
            snap_date, prices_df, fins_fy, stock_meta,
            top_n=top_n, forward_days=forward_days, progress_cb=progress_cb,
            extra_filters=extra_filters,
        )
        snap_results.append(result)

    _cb("結果集計中...")
    agg = aggregate_results(snap_results)

    return {
        "snapshots":        snapshots,
        "top_n":            top_n,
        "forward_days":     forward_days,
        "snapshot_results": snap_results,
        "aggregate":        agg,
        "data_range":       (str(p_min.date()), str(p_max.date())),
    }
