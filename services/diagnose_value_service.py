# -*- coding: utf-8 -*-
"""
バリュー株モード診断サービス（Rank IC・Top N α・重みスイープ・universe α）

成長株モード診断（diagnose_growth_service.py）と同じ規律を、
バリュー株モードに適用する。

CLAUDE.md に記載されたバリューモードの「TPX α=+18.14%」が真の選別スキルか、
filter premium（バリュー universe が TOPIX に勝つ分）の取り分かを分解する。

主要関数:
  run_diagnosis()        -- 全診断を実行（メインエントリ）
  calc_rank_ic()         -- 指標別 Spearman IC を集計
  calc_alpha_by_topn()   -- Top N 別 forward α を計算（TPX/universe の両軸）
  sweep_weights()        -- Funda/Tech 重みスイープ
  sweep_funda_variants() -- ファンダ・バリアント × 重みスイープ

データソースは pipeline_service・backtest_value_service と共通の
prices.parquet / fins_cache.parquet / stock_cache.parquet を使用。
"""

import os
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from services.backtest_value_service import _build_atdate_snapshot
from services.pipeline_service import (
    _build_fins_metrics,
    _load_fins_fy,
    _load_prices,
    _load_stock_cache,
    apply_hard_filter,
    calc_funda_score,
    calc_tech_scores,
    calc_total_score,
)

_ROOT    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_OUT_DIR = os.path.join(_ROOT, "data", "diagnose_value")

# 診断対象ファクター（バリューモード仕様）
FACTORS_FUNDA = [
    # コア value
    "PBR", "PER", "psr",
    # quality
    "ROE", "op_margin", "equity_ratio",
    # growth（参考、IC ほぼ 0 のはず）
    "rev_growth", "profit_growth", "eps_growth",
]
FACTORS_SCORE = ["funda_score", "tech_score", "total_score", "value_funda_v2"]
# 経営変化ボーナス成分（#1 分解診断）。div_trend 等は適用点(bonus_*)と単調 →
# raw 信号で代表。payout は非単調なので連続値とバンド指標の両方。activist は look-ahead。
FACTORS_BONUS = [
    "div_trend", "op_trend", "op_turnaround",
    "payout_ratio", "payout_in_band",
    "bonus_total",
    "activist",          # look-ahead（出力で明記）
]
FACTORS_ALL   = FACTORS_FUNDA + FACTORS_SCORE + FACTORS_BONUS

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
#  1スナップショットのバリュー株パイプライン再現
# ════════════════════════════════════════════════════════════════════════

def run_value_snapshot(
    as_of_date:   str,
    prices_df:    pd.DataFrame,
    fins_fy:      pd.DataFrame,
    stock_meta:   pd.DataFrame,
    top_n:        int = 20,
    forward_days: int = 60,
    progress_cb:  Optional[Callable] = None,
) -> Dict:
    """
    as_of_date 時点のバリュー株パイプラインを再現し、
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

    # バリュー株ハードフィルタ（PBR<=1.5 / PER<=25 / equity>=40% / 時価総額>100億 / rev_growth>=3%）
    _cb(f"[{as_of_date}] ハードフィルタ適用中...")
    filtered = apply_hard_filter(snap_df, fins_metrics, mode="value")
    if filtered.empty:
        return {"as_of": as_of_date, "error": "フィルタ通過銘柄なし", "n_filtered": 0}

    _cb(f"[{as_of_date}] スコア計算中... ({len(filtered)}銘柄)")
    scored = calc_funda_score(filtered, mode="value")
    scored = calc_tech_scores(scored, prices_past, mode="value")
    scored = calc_total_score(scored)

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

# Spearman 順位相関は共有 util に集約（タイ対応。旧 d² 簡易公式はタイで |IC| 過大評価）。
from services.stats_utils import spearmanr as _spearmanr


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
                  min / max / topix_mean / alpha / universe_mean / universe_alpha
    """
    if top_ns is None:
        top_ns = [3, 5, 7, 10, 15, 20, 30]

    bench_map    = _precompute_bench_returns(
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
#  value_funda_v2 計算（PBR-only スコア）
# ════════════════════════════════════════════════════════════════════════

def _add_value_funda_v2(snapshot_results: List[Dict]) -> None:
    """
    各スナップショットの scored DataFrame に `value_funda_v2` 列を追加（破壊的）。

    定義（バリューモードの実験的 funda スコア）:
      - PBR の percentile rank（低いほど高得点 = 普通の value 方向）を 1.0 倍
      - PER, PSR は使わない（PBR との冗長性確認）
      - quality / growth は混ぜない

    意図: 既存 funda_score（PBR 50pt + PSR 30pt + PER 10pt + op_margin 10pt）と、
          PBR-only スコアの IC を比較。PBR 単独で十分か、合成の意味があるかを判定。
    """
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        if "PBR" in df.columns:
            # 低いほど高得点 = ascending rank の逆を取る
            pbr_rank = df["PBR"].rank(pct=True, ascending=True, na_option="keep")
            df["value_funda_v2"] = 1.0 - pbr_rank   # 低 PBR ほど 1.0 に近い


LOO_VARIANTS = [
    "funda_score", "funda_no_activist", "funda_no_div", "funda_no_op",
    "funda_no_turnaround", "funda_no_payout", "funda_no_bonus",
    "funda_no_turn_payout",   # V字と配当性向を両方外す
]


def _add_bonus_variants(snapshot_results: List[Dict]) -> None:
    """各スナップショットの scored に leave-one-out variant 列を追加（破壊的）。
    funda_no_X = funda_score - bonus_X。bonus 列が無いスナップショットはスキップ。"""
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        if "bonus_total" not in df.columns or "funda_score" not in df.columns:
            continue
        f = df["funda_score"]
        df["funda_no_activist"]   = f - df["bonus_activist"]
        df["funda_no_div"]        = f - df["bonus_div"]
        df["funda_no_op"]         = f - df["bonus_op"]
        df["funda_no_turnaround"] = f - df["bonus_turnaround"]
        df["funda_no_payout"]     = f - df["bonus_payout"]
        df["funda_no_bonus"]      = f - df["bonus_total"]
        df["funda_no_turn_payout"] = f - df["bonus_turnaround"] - df["bonus_payout"]


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

    バリュー株モード再設計のための候補配置の網羅評価が目的。

    Parameters
    ----------
    variants : 使用する funda スコア列のリスト（snapshot_results[i]["scored"] に存在する列名）
              デフォルト: ["funda_score", "value_funda_v2"]
    weights  : (funda_w, tech_w) の組
              デフォルト: [(0.0, 1.0), (0.3, 0.7), (0.5, 0.5)]
    top_ns   : デフォルト [3, 5, 10]

    Returns
    -------
    DataFrame 列: variant / funda_w / tech_w / top_n / n / mean_return /
                  win_rate / topix_mean / alpha / universe_mean / universe_alpha
    """
    if variants is None:
        variants = ["funda_score", "value_funda_v2"]
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
    全月次スナップショットでバリュー株パイプラインを再実行し診断結果を返す。

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
      "variant_sweep"    : pd.DataFrame,
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
        r = run_value_snapshot(
            snap_date, prices_df, fins_fy, stock_meta,
            top_n=top_n, forward_days=forward_days,
        )
        snapshot_results.append(r)

        if "scored" in r:
            n_filt  = r.get("n_filtered", 0)
            n_valid = int(r["scored"]["has_fwd_data"].sum())
            _cb(f"         フィルタ通過 {n_filt}銘柄 / fwd有効 {n_valid}銘柄")
        else:
            _cb(f"         スキップ: {r.get('error', '不明')}")

    _cb("value_funda_v2 計算中...")
    _add_value_funda_v2(snapshot_results)

    _cb("経営変化ボーナス variant 計算中...")
    _add_bonus_variants(snapshot_results)

    _cb("Rank IC 集計中...")
    ic_df = calc_rank_ic(snapshot_results)

    # ボーナス成分のみ抽出した IC 表（look-ahead フラグ付き）
    bonus_ic_df = ic_df[ic_df["factor"].isin(FACTORS_BONUS)].copy()
    bonus_ic_df["look_ahead"] = bonus_ic_df["factor"].eq("activist")

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

    _cb("leave-one-out α 計算中...")
    loo_df = sweep_funda_variants(
        snapshot_results, prices_df, forward_days,
        variants=LOO_VARIANTS, weights=[(0.6, 0.4)], top_ns=[20],
    )
    # full との α 差分（= 各ボーナスの限界寄与）
    _full = loo_df.loc[loo_df["variant"] == "funda_score", "alpha"]
    _full_a = float(_full.iloc[0]) if len(_full) and pd.notna(_full.iloc[0]) else np.nan
    loo_df["alpha_drop_vs_full"] = _full_a - loo_df["alpha"]
    loo_df["look_ahead"] = loo_df["variant"].eq("funda_no_activist")

    # CSV 保存
    os.makedirs(_OUT_DIR, exist_ok=True)
    ic_df.to_csv(           os.path.join(_OUT_DIR, "ic_by_factor.csv"),        index=False, encoding="utf-8-sig")
    alpha_df.to_csv(        os.path.join(_OUT_DIR, "alpha_by_topn.csv"),       index=False, encoding="utf-8-sig")
    sweep_df.to_csv(        os.path.join(_OUT_DIR, "weight_sweep.csv"),        index=False, encoding="utf-8-sig")
    variant_sweep_df.to_csv(os.path.join(_OUT_DIR, "funda_variant_sweep.csv"), index=False, encoding="utf-8-sig")
    bonus_ic_df.to_csv(     os.path.join(_OUT_DIR, "bonus_ic.csv"),            index=False, encoding="utf-8-sig")
    loo_df.to_csv(          os.path.join(_OUT_DIR, "bonus_leave_one_out.csv"), index=False, encoding="utf-8-sig")
    _cb(f"CSV 保存完了: {_OUT_DIR}/")

    return {
        "snapshot_results": snapshot_results,
        "ic_by_factor":     ic_df,
        "alpha_by_topn":    alpha_df,
        "weight_sweep":     sweep_df,
        "variant_sweep":    variant_sweep_df,
        "bonus_ic":            bonus_ic_df,
        "bonus_leave_one_out": loo_df,
        "snapshots":        snapshots,
        "forward_days":     forward_days,
        "top_n":            top_n,
        "data_range":       (str(p_min.date()), str(p_max.date())),
    }
