# -*- coding: utf-8 -*-
"""
成長株モード診断サービス（Rank IC・Top N α・重みスイープ）

バリュー株モードが Rank IC 診断（37スナップショット）で
「ROE IC=-0.027（逆効果）」を発見して再設計したのと同じ規律で、
成長株モードの各ファンダ指標の予測力を月次スナップショットで測る。

主要関数:
  run_diagnosis()        -- 全診断を実行（メインエントリ）
  calc_rank_ic()         -- 指標別 Spearman IC を集計
  calc_alpha_by_topn()   -- Top N 別 forward α を計算
  sweep_weights()        -- Funda/Tech 重みスイープ

データソースは pipeline_service・backtest_value_service と共通の
prices.parquet / fins_cache.parquet / stock_cache.parquet を使用。
"""

import os
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from services.backtest_value_service import _build_atdate_snapshot
from services.pipeline_service import (
    _FINS_CACHE_PATH,
    _build_fins_metrics,
    _load_fins_fy,
    _load_prices,
    _load_stock_cache,
    apply_hard_filter,
    calc_funda_score,
    calc_tech_scores,
    calc_total_score,
)
from services.batch_service import _calc_sepa
from services.split_adjust import normalize_close

_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUT_DIR = os.path.join(_ROOT, "data", "diagnose_growth")

# 診断対象ファクター
FACTORS_FUNDA = [
    "rev_growth", "profit_growth", "eps_growth",
    "ROE", "op_margin", "equity_ratio",
    "PER", "PBR", "psr",
    "rev_yoy_q", "op_yoy_q",   # 直近四半期 YoY（Phase 2-C 仮説検証用）
]
FACTORS_SCORE = ["funda_score", "tech_score", "total_score", "growth_funda_v2"]
FACTORS_ALL   = FACTORS_FUNDA + FACTORS_SCORE


def _build_recent_yoy(fins_q_past: pd.DataFrame) -> pd.DataFrame:
    """as_of 時点で開示済みの最新四半期(非FY)の YoY(売上/営業益)を計算。
    四半期 Sales/OP は YTD 累計のため、同 CurPerType の前年同四半期と比較する。
    Returns DataFrame[code, rev_yoy_q, op_yoy_q]."""
    q = fins_q_past[fins_q_past["CurPerType"].isin(["1Q", "2Q", "3Q"])].copy()
    if q.empty:
        return pd.DataFrame(columns=["code", "rev_yoy_q", "op_yoy_q"])
    q["Sales"]    = pd.to_numeric(q["Sales"], errors="coerce")
    q["OP"]       = pd.to_numeric(q["OP"], errors="coerce")
    q["DiscDate"] = pd.to_datetime(q["DiscDate"], errors="coerce")

    def _yoy(c, p):
        return (c - p) / abs(p) * 100 if pd.notna(c) and pd.notna(p) and p != 0 else np.nan

    rows = []
    for code, grp in q.groupby("Code"):
        grp = grp.sort_values("DiscDate").drop_duplicates("DiscDate", keep="last")
        latest = grp.iloc[-1]
        same   = grp[grp["CurPerType"] == latest["CurPerType"]]
        # 同 CurPerType で 300 日以上前の最新 = 前年同四半期（訂正再開示の重複を回避）
        prior  = same[same["DiscDate"] <= latest["DiscDate"] - pd.Timedelta(days=300)]
        if prior.empty:
            continue
        prev = prior.iloc[-1]
        rows.append({
            "code":      code,
            "rev_yoy_q": _yoy(latest["Sales"], prev["Sales"]),
            "op_yoy_q":  _yoy(latest["OP"],    prev["OP"]),
        })
    return pd.DataFrame(rows, columns=["code", "rev_yoy_q", "op_yoy_q"])

BENCHMARK_CODE = "13060"   # TOPIX連動ETF (1306)


# ════════════════════════════════════════════════════════════════════════
#  月次スナップショット日リスト生成
# ════════════════════════════════════════════════════════════════════════

def generate_monthly_snapshots(
    p_min: pd.Timestamp,
    p_max: pd.Timestamp,
    forward_days: int = 60,
) -> List[str]:
    """
    月末日リストを生成する。
    forward_days 後のリターンが計算できる範囲のみ返す。
    """
    latest_valid = p_max - pd.Timedelta(days=forward_days)
    dates: List[str] = []
    # p_min が属する月の末日から開始
    current = p_min + pd.offsets.MonthEnd(0)
    while current <= latest_valid:
        dates.append(current.strftime("%Y-%m-%d"))
        current = current + pd.offsets.MonthEnd(1)
    return dates


# ════════════════════════════════════════════════════════════════════════
#  forward リターン計算（分割対応）
# ════════════════════════════════════════════════════════════════════════

def _calc_fwd_returns(
    prices_df: pd.DataFrame,
    as_of: pd.Timestamp,
    forward_days: int,
    codes: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    forward_days 後の価格を as_of スケールに統一して返す。

    - 生 C を使用（AdjC はスナップショット混在のため不可）
    - 分割係数: as_of < date <= fwd_date 区間の AdjFactor 累積積で ÷ して
      as_of スケールに揃える

    Parameters
    ----------
    codes : フィルタ後の銘柄コードリスト（省略時は全銘柄）

    Returns
    -------
    DataFrame[code, fwd_date, price_fwd]
    """
    fwd_target = as_of + pd.Timedelta(days=forward_days)
    p = prices_df.copy()
    p["Date"]      = pd.to_datetime(p["Date"], errors="coerce")
    p["AdjFactor"] = pd.to_numeric(p["AdjFactor"], errors="coerce").fillna(1.0)

    if codes is not None:
        p = p[p["Code"].isin(set(codes))]

    p_fwd = p[p["Date"] <= fwd_target].sort_values(["Code", "Date"])

    rows = []
    for code, grp in p_fwd.groupby("Code"):
        grp = grp.sort_values("Date").reset_index(drop=True)
        last_row    = grp.iloc[-1]
        fwd_date    = last_row["Date"]
        fwd_C_raw   = pd.to_numeric(last_row["C"], errors="coerce")

        # as_of < date <= fwd_date の累積分割係数
        in_window   = grp[(grp["Date"] > as_of) & (grp["Date"] <= fwd_date)]
        sf          = float(in_window["AdjFactor"].prod()) if len(in_window) > 0 else 1.0

        price_fwd = fwd_C_raw / sf if (pd.notna(fwd_C_raw) and sf > 0) else np.nan
        rows.append({"code": code, "fwd_date": fwd_date, "price_fwd": price_fwd})

    if not rows:
        return pd.DataFrame(columns=["code", "fwd_date", "price_fwd"])
    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════════════
#  1スナップショットの成長株パイプライン再現
# ════════════════════════════════════════════════════════════════════════

def run_growth_snapshot(
    as_of_date:   str,
    prices_df:    pd.DataFrame,
    fins_fy:      pd.DataFrame,
    stock_meta:   pd.DataFrame,
    top_n:        int = 20,
    forward_days: int = 60,
    progress_cb:  Optional[Callable] = None,
    fins_all:     Optional[pd.DataFrame] = None,
) -> Dict:
    """
    as_of_date 時点の成長株パイプラインを再現し、
    forward_days 後のリターン付き scored DataFrame を返す。

    Look-ahead bias 対策:
      - prices_past  : prices_df の as_of 以前のみ
      - fins_past    : fins_fy  の DiscDate <= as_of のみ
      - _build_atdate_snapshot が per-share 値の分割スケールを統一
    """
    def _cb(m):
        if progress_cb:
            progress_cb(m)

    as_of      = pd.Timestamp(as_of_date)
    fwd_target = as_of + pd.Timedelta(days=forward_days)

    # as_of 以前のデータだけ使う
    p = prices_df.copy()
    p["Date"] = pd.to_datetime(p["Date"], errors="coerce")
    prices_past = p[p["Date"] <= as_of].copy()
    fins_past   = fins_fy[fins_fy["DiscDate"] <= as_of].copy()

    if prices_past.empty or fins_past.empty:
        return {"as_of": as_of_date, "error": "データ不足"}

    # スナップショット構築（close / PER / PBR / ROE / rev_growth / profit_growth）
    _cb(f"[{as_of_date}] スナップショット構築中...")
    snap_df = _build_atdate_snapshot(prices_past, fins_past, stock_meta, as_of)
    if snap_df.empty:
        return {"as_of": as_of_date, "error": "スナップショット空"}

    # 追加財務指標（eps_growth / op_margin / equity_ratio / sh_out / sales_fy）
    _cb(f"[{as_of_date}] 財務指標計算中...")
    fins_metrics = _build_fins_metrics(fins_past, prices_past)

    # 成長株ハードフィルタ（売上>10% / 利益>10% / ROE>15% / equity>30% / 時価総額>100億）
    _cb(f"[{as_of_date}] ハードフィルタ適用中...")
    filtered = apply_hard_filter(snap_df, fins_metrics, mode="growth")
    if filtered.empty:
        return {"as_of": as_of_date, "error": "フィルタ通過銘柄なし", "n_filtered": 0}

    _cb(f"[{as_of_date}] スコア計算中... ({len(filtered)}銘柄)")
    scored = calc_funda_score(filtered, mode="growth")
    # 過去時点の SEPA stage を計算して付与（calc_tech_scores が参照）。
    # topix は簡易省略（RS 条件のみ欠落、stage 大枠は live と一致）。
    _stages = []
    for _code in scored["code"]:
        _cp = prices_past[prices_past["Code"] == _code]
        if len(_cp) >= 200:
            _stages.append(_calc_sepa(normalize_close(_cp.sort_values("Date")), None).get("sepa_stage", 0))
        else:
            _stages.append(0)
    scored["sepa_stage"] = _stages
    scored = calc_tech_scores(scored, prices_past, mode="growth")
    scored = calc_total_score(scored)

    # 直近四半期 YoY(売上/営業益) を付与（Rank IC 計測用・Phase 2-C 仮説検証）
    if fins_all is not None:
        _fa = fins_all.copy()
        _fa["DiscDate"] = pd.to_datetime(_fa["DiscDate"], errors="coerce")
        yoy = _build_recent_yoy(_fa[_fa["DiscDate"] <= as_of])
        scored = scored.merge(yoy, on="code", how="left")

    # forward リターン（フィルタ通過銘柄のみ計算）
    _cb(f"[{as_of_date}] forward リターン計算中...")
    fwd_df = _calc_fwd_returns(
        p, as_of, forward_days,
        codes=scored["code"].tolist(),
    )
    scored = scored.merge(fwd_df, on="code", how="left")
    scored["return_pct"] = (scored["price_fwd"] / scored["close"] - 1) * 100

    # forward データが揃っているかフラグ
    scored["has_fwd_data"] = (
        scored["fwd_date"].notna()
        & (scored["fwd_date"] >= fwd_target - pd.Timedelta(days=14))
        & scored["return_pct"].notna()
    )

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
#  Rank IC 計算
# ════════════════════════════════════════════════════════════════════════

def _spearmanr(x: np.ndarray, y: np.ndarray) -> float:
    """NaN を除外した Spearman 相関係数（scipy 不要）。有効サンプル < 5 は NaN。"""
    mask = ~(np.isnan(x) | np.isnan(y))
    if mask.sum() < 5:
        return np.nan
    x_, y_ = x[mask], y[mask]
    n  = len(x_)
    xr = pd.Series(x_).rank().values
    yr = pd.Series(y_).rank().values
    d2 = float(np.sum((xr - yr) ** 2))
    denom = n * (n ** 2 - 1)
    return float(1.0 - 6.0 * d2 / denom) if denom > 0 else np.nan


def calc_rank_ic(
    snapshot_results: List[Dict],
    factors: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    各 factor と forward return の Spearman 相関を月次集計する。

    Returns
    -------
    DataFrame 列:
      factor / mean_ic / median_ic / pos_months / total_months / pos_pct / t_stat
    mean_ic の降順でソート。IC > 0.10 で実用レベル。
    """
    if factors is None:
        factors = FACTORS_ALL

    ic_records: List[Dict] = []
    for r in snapshot_results:
        if "scored" not in r:
            continue
        valid = r["scored"][r["scored"]["has_fwd_data"]].copy()
        if len(valid) < 5:
            continue
        ret = valid["return_pct"].values.astype(float)
        row: Dict = {"snapshot": r["as_of"]}
        for f in factors:
            if f not in valid.columns:
                row[f] = np.nan
                continue
            x = valid[f].values.astype(float)
            row[f] = _spearmanr(x, ret)
        ic_records.append(row)

    if not ic_records:
        return pd.DataFrame()

    ic_df = pd.DataFrame(ic_records).set_index("snapshot")

    rows = []
    for f in factors:
        if f not in ic_df.columns:
            continue
        s = ic_df[f].dropna()
        if len(s) == 0:
            continue
        n        = len(s)
        mean_ic  = float(s.mean())
        median_ic = float(s.median())
        pos      = int((s > 0).sum())
        std_ic   = float(s.std()) if n > 1 else 0.0
        t_stat   = mean_ic / (std_ic / np.sqrt(n)) if (std_ic > 0 and n > 1) else np.nan
        rows.append({
            "factor":        f,
            "mean_ic":       round(mean_ic,   4),
            "median_ic":     round(median_ic, 4),
            "pos_months":    pos,
            "total_months":  n,
            "pos_pct":       round(pos / n * 100, 1),
            "t_stat":        round(t_stat, 2) if not np.isnan(t_stat) else None,
        })

    return (pd.DataFrame(rows)
              .sort_values("mean_ic", ascending=False)
              .reset_index(drop=True))


# ════════════════════════════════════════════════════════════════════════
#  ベンチマーク forward リターン（TOPIX ETF）
# ════════════════════════════════════════════════════════════════════════

def _bench_return(
    prices_df:      pd.DataFrame,
    as_of:          pd.Timestamp,
    forward_days:   int,
    benchmark_code: str = BENCHMARK_CODE,
) -> float:
    """ベンチマーク ETF の forward_days 後リターン（%、分割対応）。"""
    p = prices_df.copy()
    p["Date"]      = pd.to_datetime(p["Date"], errors="coerce")
    p["AdjFactor"] = pd.to_numeric(p["AdjFactor"], errors="coerce").fillna(1.0)

    bench      = p[p["Code"] == benchmark_code].sort_values("Date")
    fwd_target = as_of + pd.Timedelta(days=forward_days)
    before     = bench[bench["Date"] <= as_of]
    after      = bench[bench["Date"] <= fwd_target]

    if before.empty or after.empty:
        return np.nan

    close_base    = pd.to_numeric(before.iloc[-1]["C"], errors="coerce")
    last_fwd      = after.iloc[-1]
    close_fwd_raw = pd.to_numeric(last_fwd["C"], errors="coerce")
    fwd_date      = last_fwd["Date"]

    in_window = bench[(bench["Date"] > as_of) & (bench["Date"] <= fwd_date)]
    sf        = float(in_window["AdjFactor"].prod()) if len(in_window) > 0 else 1.0

    if pd.notna(close_base) and pd.notna(close_fwd_raw) and close_base > 0 and sf > 0:
        return (close_fwd_raw / sf - close_base) / close_base * 100
    return np.nan


def _precompute_bench_returns(
    snapshot_results: List[Dict],
    prices_df:        pd.DataFrame,
    forward_days:     int,
    benchmark_code:   str = BENCHMARK_CODE,
) -> Dict[str, float]:
    return {
        r["as_of"]: _bench_return(
            prices_df, pd.Timestamp(r["as_of"]), forward_days, benchmark_code
        )
        for r in snapshot_results
        if "scored" in r
    }


def _precompute_universe_returns(snapshot_results: List[Dict]) -> Dict[str, float]:
    """
    各スナップショットの「フィルタ通過銘柄 equal-weight 平均 forward リターン」を返す。

    has_fwd_data=True の銘柄全件の return_pct の単純平均。
    これは「stock-picking しなかった場合のユニバース・ベースライン」となる。

    Returns
    -------
    {as_of_date: universe_mean_return_pct}
    """
    out: Dict[str, float] = {}
    for r in snapshot_results:
        if "scored" not in r:
            continue
        valid = r["scored"][r["scored"]["has_fwd_data"]]
        if len(valid) > 0:
            out[r["as_of"]] = float(valid["return_pct"].mean())
    return out


# ════════════════════════════════════════════════════════════════════════
#  Top N 別 α 分析
# ════════════════════════════════════════════════════════════════════════

def calc_alpha_by_topn(
    snapshot_results: List[Dict],
    prices_df:        pd.DataFrame,
    forward_days:     int,
    top_ns:           Optional[List[int]] = None,
    benchmark_code:   str = BENCHMARK_CODE,
) -> pd.DataFrame:
    """
    Top N 別の forward リターン統計と TOPIX 対比 α を計算する。

    Returns
    -------
    DataFrame 列: top_n / n / mean_return / median_return / win_rate /
                  min / max / topix_mean / alpha
    """
    if top_ns is None:
        top_ns = [3, 5, 7, 10, 15, 20, 30]

    bench_map   = _precompute_bench_returns(
        snapshot_results, prices_df, forward_days, benchmark_code
    )
    universe_map = _precompute_universe_returns(snapshot_results)

    rows = []
    for top_n in top_ns:
        top_rets:     List[float] = []
        bench_rets:   List[float] = []
        universe_rets: List[float] = []

        for r in snapshot_results:
            if "scored" not in r:
                continue
            valid     = r["scored"][r["scored"]["has_fwd_data"]].copy()
            top_valid = valid[valid["rank"] <= top_n]["return_pct"].dropna()
            if top_valid.empty:
                continue
            top_rets.extend(top_valid.tolist())
            bret = bench_map.get(r["as_of"], np.nan)
            if not np.isnan(bret):
                bench_rets.extend([bret] * len(top_valid))
            uret = universe_map.get(r["as_of"], np.nan)
            if not np.isnan(uret):
                universe_rets.extend([uret] * len(top_valid))

        if not top_rets:
            rows.append({"top_n": top_n, "n": 0, "mean_return": None,
                         "median_return": None, "win_rate": None,
                         "min": None, "max": None,
                         "topix_mean": None, "alpha": None,
                         "universe_mean": None, "universe_alpha": None})
            continue

        s             = pd.Series(top_rets)
        bench_mean    = float(np.nanmean(bench_rets))    if bench_rets    else np.nan
        universe_mean = float(np.nanmean(universe_rets)) if universe_rets else np.nan
        mean_ret      = round(float(s.mean()), 2)
        rows.append({
            "top_n":          top_n,
            "n":              int(len(s)),
            "mean_return":    mean_ret,
            "median_return":  round(float(s.median()), 2),
            "win_rate":       round(float((s > 0).sum()) / len(s) * 100, 1),
            "min":            round(float(s.min()), 2),
            "max":            round(float(s.max()), 2),
            "topix_mean":     round(bench_mean, 2)    if not np.isnan(bench_mean)    else None,
            "alpha":          round(mean_ret - bench_mean, 2)
                              if not np.isnan(bench_mean) else None,
            "universe_mean":  round(universe_mean, 2) if not np.isnan(universe_mean) else None,
            "universe_alpha": round(mean_ret - universe_mean, 2)
                              if not np.isnan(universe_mean) else None,
        })

    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════════════
#  Funda / Tech 重みスイープ
# ════════════════════════════════════════════════════════════════════════

def sweep_weights(
    snapshot_results: List[Dict],
    prices_df:        pd.DataFrame,
    forward_days:     int,
    weights:          Optional[List[Tuple[float, float]]] = None,
    top_n:            int = 20,
    benchmark_code:   str = BENCHMARK_CODE,
) -> pd.DataFrame:
    """
    Funda/Tech 重みの組み合わせごとに Top N α を計算する。

    Parameters
    ----------
    weights : [(funda_w, tech_w), ...] リスト
    """
    if weights is None:
        weights = [(0.0, 1.0), (0.2, 0.8), (0.3, 0.7), (0.4, 0.6), (0.5, 0.5), (0.6, 0.4), (0.7, 0.3), (1.0, 0.0)]

    bench_map    = _precompute_bench_returns(
        snapshot_results, prices_df, forward_days, benchmark_code
    )
    universe_map = _precompute_universe_returns(snapshot_results)

    rows = []
    for fw, tw in weights:
        top_rets:     List[float] = []
        bench_rets:   List[float] = []
        universe_rets: List[float] = []

        for r in snapshot_results:
            if "scored" not in r:
                continue
            df = r["scored"].copy()
            # 重みで total_score を再計算し Top N を選び直す
            df["_total_w"] = (
                df["funda_score"].fillna(0) * fw
                + df["tech_score"].fillna(0) * tw
            )
            valid     = df[df["has_fwd_data"]].copy()
            top_valid = (valid.sort_values("_total_w", ascending=False)
                              .head(top_n)["return_pct"].dropna())
            if top_valid.empty:
                continue
            top_rets.extend(top_valid.tolist())
            bret = bench_map.get(r["as_of"], np.nan)
            if not np.isnan(bret):
                bench_rets.extend([bret] * len(top_valid))
            uret = universe_map.get(r["as_of"], np.nan)
            if not np.isnan(uret):
                universe_rets.extend([uret] * len(top_valid))

        if not top_rets:
            rows.append({"funda_w": fw, "tech_w": tw, "n": 0,
                         "mean_return": None, "alpha": None, "win_rate": None,
                         "topix_mean": None,
                         "universe_mean": None, "universe_alpha": None})
            continue

        s             = pd.Series(top_rets)
        bench_mean    = float(np.nanmean(bench_rets))    if bench_rets    else np.nan
        universe_mean = float(np.nanmean(universe_rets)) if universe_rets else np.nan
        mean_ret      = round(float(s.mean()), 2)
        rows.append({
            "funda_w":        fw,
            "tech_w":         tw,
            "n":              int(len(s)),
            "mean_return":    mean_ret,
            "median_return":  round(float(s.median()), 2),
            "win_rate":       round(float((s > 0).sum()) / len(s) * 100, 1),
            "topix_mean":     round(bench_mean, 2)    if not np.isnan(bench_mean)    else None,
            "alpha":          round(mean_ret - bench_mean, 2)
                              if not np.isnan(bench_mean) else None,
            "universe_mean":  round(universe_mean, 2) if not np.isnan(universe_mean) else None,
            "universe_alpha": round(mean_ret - universe_mean, 2)
                              if not np.isnan(universe_mean) else None,
        })

    return pd.DataFrame(rows)


# ════════════════════════════════════════════════════════════════════════
#  growth_funda_v2 計算（バリュー指標反転スコア）
# ════════════════════════════════════════════════════════════════════════

def _add_growth_funda_v2(snapshot_results: List[Dict]) -> None:
    """
    各スナップショットの scored DataFrame に `growth_funda_v2` 列を追加（破壊的）。

    定義（成長モードに最適化した実験的 funda スコア）:
      - PBR / PER / psr を percentile rank で入れる（高いほど高得点 = バリュー指標を反転）
      - eps_growth / profit_growth を percentile rank で入れる（高いほど高得点）
      - 上記の単純平均（0〜1 にスケール）

    意図: PBR/PER/PSR の負 IC は経済的に「成長 universe 内では高 PBR がモメンタム
          シグナル」として解釈できる。これを順方向に取り込んだら IC は改善するか？
    """
    cols_used = ["PBR", "PER", "psr", "eps_growth", "profit_growth"]
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        ranks: List[pd.Series] = []
        for col in cols_used:
            if col in df.columns:
                ranks.append(df[col].rank(pct=True, na_option="keep"))
        if ranks:
            df["growth_funda_v2"] = sum(ranks) / len(ranks)


# ════════════════════════════════════════════════════════════════════════
#  ファンダ・バリアント × 重みスイープ
# ════════════════════════════════════════════════════════════════════════

def sweep_funda_variants(
    snapshot_results: List[Dict],
    prices_df:        pd.DataFrame,
    forward_days:     int,
    variants:         Optional[List[str]] = None,
    weights:          Optional[List[Tuple[float, float]]] = None,
    top_ns:           Optional[List[int]] = None,
    benchmark_code:   str = BENCHMARK_CODE,
) -> pd.DataFrame:
    """
    funda スコアのバリアント × Funda/Tech 重み × Top N で Top N alpha を網羅計算する。

    Phase 2A 再設計のための候補配置の網羅評価が目的。

    Parameters
    ----------
    variants : 使用する funda スコア列のリスト（snapshot_results[i]["scored"] に存在する列名）
              デフォルト: ["funda_score", "growth_funda_v2"]
    weights  : (funda_w, tech_w) の組
              デフォルト: [(0.0, 1.0), (0.3, 0.7), (0.5, 0.5)]
    top_ns   : デフォルト [3, 5, 10]

    Returns
    -------
    DataFrame 列: variant / funda_w / tech_w / top_n / n / mean_return /
                  win_rate / topix_mean / alpha
    """
    if variants is None:
        variants = ["funda_score", "growth_funda_v2"]
    if weights is None:
        weights = [(0.0, 1.0), (0.3, 0.7), (0.5, 0.5)]
    if top_ns is None:
        top_ns = [3, 5, 10]

    bench_map    = _precompute_bench_returns(
        snapshot_results, prices_df, forward_days, benchmark_code
    )
    universe_map = _precompute_universe_returns(snapshot_results)

    rows = []
    for variant in variants:
        for fw, tw in weights:
            for top_n in top_ns:
                top_rets:     List[float] = []
                bench_rets:   List[float] = []
                universe_rets: List[float] = []
                for r in snapshot_results:
                    if "scored" not in r:
                        continue
                    df = r["scored"].copy()
                    if variant not in df.columns:
                        continue
                    df["_total_w"] = (
                        df[variant].fillna(0)      * fw
                      + df["tech_score"].fillna(0) * tw
                    )
                    valid = df[df["has_fwd_data"]].copy()
                    top_valid = (valid.sort_values("_total_w", ascending=False)
                                      .head(top_n)["return_pct"].dropna())
                    if top_valid.empty:
                        continue
                    top_rets.extend(top_valid.tolist())
                    bret = bench_map.get(r["as_of"], np.nan)
                    if not np.isnan(bret):
                        bench_rets.extend([bret] * len(top_valid))
                    uret = universe_map.get(r["as_of"], np.nan)
                    if not np.isnan(uret):
                        universe_rets.extend([uret] * len(top_valid))

                if not top_rets:
                    rows.append({"variant": variant, "funda_w": fw, "tech_w": tw,
                                 "top_n": top_n, "n": 0,
                                 "mean_return": None, "win_rate": None,
                                 "topix_mean": None, "alpha": None,
                                 "universe_mean": None, "universe_alpha": None})
                    continue

                s             = pd.Series(top_rets)
                bench_mean    = float(np.nanmean(bench_rets))    if bench_rets    else np.nan
                universe_mean = float(np.nanmean(universe_rets)) if universe_rets else np.nan
                mean_ret      = round(float(s.mean()), 2)
                rows.append({
                    "variant":        variant,
                    "funda_w":        fw,
                    "tech_w":         tw,
                    "top_n":          top_n,
                    "n":              int(len(s)),
                    "mean_return":    mean_ret,
                    "win_rate":       round(float((s > 0).sum()) / len(s) * 100, 1),
                    "topix_mean":     round(bench_mean, 2)    if not np.isnan(bench_mean)    else None,
                    "alpha":          round(mean_ret - bench_mean, 2)
                                      if not np.isnan(bench_mean) else None,
                    "universe_mean":  round(universe_mean, 2) if not np.isnan(universe_mean) else None,
                    "universe_alpha": round(mean_ret - universe_mean, 2)
                                      if not np.isnan(universe_mean) else None,
                })

    return (pd.DataFrame(rows)
              .sort_values("universe_alpha", ascending=False, na_position="last")
              .reset_index(drop=True))


# ════════════════════════════════════════════════════════════════════════
#  メインエントリ
# ════════════════════════════════════════════════════════════════════════

def run_diagnosis(
    forward_days:   int = 60,
    top_n:          int = 20,
    max_snapshots:  Optional[int] = None,
    progress_cb:    Optional[Callable] = None,
) -> Dict:
    """
    全月次スナップショットで成長株パイプラインを再実行し診断結果を返す。

    Parameters
    ----------
    forward_days   : forward リターンの計算期間（日）
    top_n          : Top N α 分析の基準 N
    max_snapshots  : 直近 N 件に限定（None = 全件）
    progress_cb    : 進捗コールバック（str を受け取る callable）

    Returns
    -------
    {
      "snapshot_results" : List[Dict],
      "ic_by_factor"     : pd.DataFrame,
      "alpha_by_topn"    : pd.DataFrame,
      "weight_sweep"     : pd.DataFrame,
      "snapshots"        : List[str],
      "forward_days"     : int,
      "top_n"            : int,
      "data_range"       : (str, str),
    }
    """
    def _cb(m: str) -> None:
        if progress_cb:
            progress_cb(m)

    _cb("データ読み込み中...")
    prices_df  = _load_prices()
    fins_fy    = _load_fins_fy()
    fins_all   = pd.read_parquet(_FINS_CACHE_PATH)
    stock_meta = _load_stock_cache()

    prices_df["Date"] = pd.to_datetime(prices_df["Date"], errors="coerce")
    p_min = prices_df["Date"].min()
    p_max = prices_df["Date"].max()
    _cb(f"価格データ範囲: {p_min.date()} 〜 {p_max.date()}")

    snapshots = generate_monthly_snapshots(p_min, p_max, forward_days)
    if max_snapshots:
        snapshots = snapshots[-max_snapshots:]
    _cb(f"スナップショット: {len(snapshots)} 件 ({snapshots[0]} 〜 {snapshots[-1]})")

    snapshot_results: List[Dict] = []
    for i, snap_date in enumerate(snapshots):
        _cb(f"[{i+1:>2}/{len(snapshots)}] {snap_date}")
        r = run_growth_snapshot(
            snap_date, prices_df, fins_fy, stock_meta,
            top_n=top_n, forward_days=forward_days,
            fins_all=fins_all,
        )
        snapshot_results.append(r)

        if "scored" in r:
            n_filt  = r.get("n_filtered", 0)
            n_valid = int(r["scored"]["has_fwd_data"].sum())
            _cb(f"         フィルタ通過 {n_filt}銘柄 / fwd有効 {n_valid}銘柄")
        else:
            _cb(f"         スキップ: {r.get('error', '不明')}")

    _cb("growth_funda_v2 計算中...")
    _add_growth_funda_v2(snapshot_results)

    _cb("Rank IC 集計中...")
    ic_df = calc_rank_ic(snapshot_results)

    _cb("Top N α 計算中...")
    alpha_df = calc_alpha_by_topn(
        snapshot_results, prices_df, forward_days,
    )

    _cb("重みスイープ計算中...")
    sweep_df = sweep_weights(
        snapshot_results, prices_df, forward_days, top_n=top_n,
    )

    _cb("ファンダ・バリアント × 重みスイープ計算中...")
    variant_sweep_df = sweep_funda_variants(
        snapshot_results, prices_df, forward_days,
    )

    # CSV 保存
    os.makedirs(_OUT_DIR, exist_ok=True)
    ic_df.to_csv(           os.path.join(_OUT_DIR, "ic_by_factor.csv"),        index=False, encoding="utf-8-sig")
    alpha_df.to_csv(        os.path.join(_OUT_DIR, "alpha_by_topn.csv"),       index=False, encoding="utf-8-sig")
    sweep_df.to_csv(        os.path.join(_OUT_DIR, "weight_sweep.csv"),        index=False, encoding="utf-8-sig")
    variant_sweep_df.to_csv(os.path.join(_OUT_DIR, "funda_variant_sweep.csv"), index=False, encoding="utf-8-sig")
    _cb(f"CSV 保存完了: {_OUT_DIR}/")

    return {
        "snapshot_results": snapshot_results,
        "ic_by_factor":     ic_df,
        "alpha_by_topn":    alpha_df,
        "weight_sweep":     sweep_df,
        "variant_sweep":    variant_sweep_df,
        "snapshots":        snapshots,
        "forward_days":     forward_days,
        "top_n":            top_n,
        "data_range":       (str(p_min.date()), str(p_max.date())),
    }
