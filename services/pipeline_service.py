# -*- coding: utf-8 -*-
"""
パイプラインサービス：3段階スクリーニング + Claude API 分析

フロー:
  ① ハードフィルタ（全3800銘柄 → 約50銘柄）
  ② ファンダスコア  Growth50% + Quality25% + Value25%（パーセンタイル正規化 0〜100）
  ③ テクニカルスコア MA30 + RSI20 + MACD20 + Volume15 + Breakout15（0〜100）
  Final = Funda × 0.60 + Technical × 0.40
  上位10銘柄 → Claude API 分析
"""

import os
import json
import re
import math
from datetime import datetime

import anthropic
import numpy as np
import pandas as pd

_ROOT             = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_STOCK_CACHE_PATH = os.path.join(_ROOT, "data", "stock_cache.parquet")
_FINS_CACHE_PATH  = os.path.join(_ROOT, "data", "fins_cache.parquet")
_PRICES_PATH      = os.path.join(_ROOT, "data", "prices.parquet")

# ── モデル・コスト ────────────────────────────────────────────────────────
MODEL                 = "claude-sonnet-4-6"
INPUT_COST_PER_TOKEN  = 3.00  / 1_000_000   # $3.00/MTok
OUTPUT_COST_PER_TOKEN = 15.00 / 1_000_000   # $15.00/MTok

# ── ハードフィルタ閾値（成長株モード） ───────────────────────────────────
HARD = {
    "rev_growth_min":    10.0,    # 売上成長率 > 10%
    "profit_growth_min": 10.0,    # 利益成長率（純利益）> 10%
    "roe_min":           15.0,    # ROE > 15%
    "equity_ratio_min":  30.0,    # 自己資本比率 > 30%
    "market_cap_min":    100e8,   # 時価総額 > 100億円
}

# ── ハードフィルタ閾値（バリュー株モード） ──────────────────────────────
HARD_VALUE = {
    "pbr_max":           1.5,     # PBR ≤ 1.5
    "per_max":           20.0,    # PER ≤ 20（かつ正値）
    "roe_min":            8.0,    # ROE ≥ 8%
    "equity_ratio_min":  40.0,    # 自己資本比率 ≥ 40%
    "market_cap_min":   100e8,    # 時価総額 > 100億円
    "rev_growth_min":    3.0,     # 売上成長率 ≥ 3%（死に株排除）
}

# ── ファンダ各指標の最大点数（成長株モード） ─────────────────────────────
FUNDA_MAX = {
    # Growth 50pt
    "rev_growth":    20,
    "profit_growth": 20,
    "eps_growth":    10,
    # Quality 25pt
    "roe":           10,
    "op_margin":     10,
    "equity_ratio":   5,
    # Value 25pt（低いほど高評価 → 逆パーセンタイル）
    "per":           10,
    "psr":           10,
    "pbr":            5,
}
VALUE_METRICS = {"per", "psr", "pbr"}   # 低いほど良い指標

# ── ファンダ各指標の最大点数（バリュー株モード） ─────────────────────────
FUNDA_MAX_VALUE = {
    # Value 50pt（割安指標を重視）
    "pbr":           20,
    "per":           20,
    "psr":           10,
    # Quality 25pt
    "roe":           10,
    "op_margin":     10,
    "equity_ratio":   5,
    # Growth 25pt（成長は補助的）
    "rev_growth":    10,
    "profit_growth": 10,
    "eps_growth":     5,
}
VALUE_METRICS_VALUE = {"per", "psr", "pbr"}   # 低いほど良い指標（バリューモード）


# ════════════════════════════════════════════════════════════════════════
#  データ読み込み
# ════════════════════════════════════════════════════════════════════════

def _load_stock_cache() -> pd.DataFrame:
    return pd.read_parquet(_STOCK_CACHE_PATH)

def _load_prices() -> pd.DataFrame:
    return pd.read_parquet(_PRICES_PATH)

def _load_fins_fy() -> pd.DataFrame:
    """fins_cache から年度（FY）決算レコードのみ返す。"""
    df = pd.read_parquet(_FINS_CACHE_PATH)
    fy = df[df["CurPerType"] == "FY"].copy()
    # 数値型に変換
    for col in ["Sales", "OP", "NP", "EPS", "Eq", "EqAR", "ShOutFY", "CFO"]:
        fy[col] = pd.to_numeric(fy[col], errors="coerce")
    fy["DiscDate"] = pd.to_datetime(fy["DiscDate"], errors="coerce")
    return fy.sort_values(["Code", "DiscDate"], ascending=[True, False])


# ════════════════════════════════════════════════════════════════════════
#  ① ハードフィルタ
# ════════════════════════════════════════════════════════════════════════

def _build_fins_metrics(fins_fy: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    fins_fy から銘柄ごとに最新FYと前FYを比較して
    eps_growth / op_margin / equity_ratio / market_cap_base を返す。
    prices_df を使い、EPS開示後の株式分割に対応したsh_outを計算する。
    """
    # 事前にコード別グループ化（ループ内での全行検索を排除）
    prices_df = prices_df.copy()
    prices_df["Date"]      = pd.to_datetime(prices_df["Date"], errors="coerce")
    prices_df["AdjFactor"] = pd.to_numeric(prices_df["AdjFactor"], errors="coerce").fillna(1.0)
    prices_grouped = {code: grp for code, grp in prices_df.groupby("Code")}

    rows = []
    for code, grp in fins_fy.groupby("Code"):
        grp = grp.reset_index(drop=True)
        if len(grp) < 1:
            continue
        curr = grp.iloc[0]

        eps_g     = np.nan
        op_margin = np.nan

        if len(grp) >= 2:
            prev  = grp.iloc[1]
            eps_c = curr["EPS"]
            eps_p = prev["EPS"]
            if pd.notna(eps_c) and pd.notna(eps_p) and eps_p != 0:
                eps_g = (eps_c - eps_p) / abs(eps_p) * 100

        sales = curr["Sales"]
        op    = curr["OP"]
        if pd.notna(sales) and pd.notna(op) and sales > 0:
            op_margin = op / sales * 100

        eq_ar    = curr["EqAR"]
        equity_r = eq_ar * 100 if pd.notna(eq_ar) else np.nan

        # 株式分割対応: EPS開示日後のAdjFactor累積積でsh_outを調整
        sh_out    = curr["ShOutFY"]
        disc_date = pd.to_datetime(curr.get("DiscDate"), errors="coerce")
        if pd.notna(disc_date) and pd.notna(sh_out):
            cp = prices_grouped.get(code)
            if cp is not None:
                adj_after = cp.loc[cp["Date"] > disc_date, "AdjFactor"]
                if len(adj_after) > 0:
                    split_factor = float(adj_after.prod())
                    if split_factor > 0:
                        sh_out = sh_out / split_factor

        # 連続増配チェック（2期連続増配=2, 1期増配=1, なし=0）
        div_trend = 0
        d_curr = pd.to_numeric(curr.get("DivAnn"), errors="coerce")
        if len(grp) >= 2 and pd.notna(d_curr) and d_curr > 0:
            d_prev1 = pd.to_numeric(grp.iloc[1].get("DivAnn"), errors="coerce")
            if pd.notna(d_prev1) and d_prev1 > 0 and d_curr > d_prev1:
                div_trend = 1
                if len(grp) >= 3:
                    d_prev2 = pd.to_numeric(grp.iloc[2].get("DivAnn"), errors="coerce")
                    if pd.notna(d_prev2) and d_prev2 > 0 and d_prev1 > d_prev2:
                        div_trend = 2  # 2期連続増配

        # 営業利益トレンド（2期連続増=2, 1期増=1, 横ばい/減=0）
        # op_turnaround: 前期減益→今期回復（V字転換）= True
        op_trend      = 0
        op_turnaround = False
        op_curr = pd.to_numeric(curr.get("OP"), errors="coerce")
        if len(grp) >= 2 and pd.notna(op_curr):
            op_prev1 = pd.to_numeric(grp.iloc[1].get("OP"), errors="coerce")
            if pd.notna(op_prev1) and op_prev1 > 0 and op_curr >= op_prev1:
                op_trend = 1
                if len(grp) >= 3:
                    op_prev2 = pd.to_numeric(grp.iloc[2].get("OP"), errors="coerce")
                    if pd.notna(op_prev2) and op_prev2 > 0:
                        if op_prev1 >= op_prev2:
                            op_trend = 2       # 2期連続増加
                        else:
                            op_turnaround = True  # 前期減益→今期回復（V字転換）

        # 配当性向（DivAnn ÷ EPS × 100）
        payout_ratio = np.nan
        eps_curr = pd.to_numeric(curr.get("EPS"),    errors="coerce")
        div_curr = pd.to_numeric(curr.get("DivAnn"), errors="coerce")
        if pd.notna(eps_curr) and pd.notna(div_curr) and eps_curr > 0:
            payout_ratio = div_curr / eps_curr * 100

        rows.append({
            "code":           code,
            "eps_growth":     eps_g,
            "op_margin":      op_margin,
            "equity_ratio":   equity_r,
            "sh_out":         sh_out,
            "sales_fy":       curr["Sales"],
            "div_trend":      div_trend,
            "op_trend":       op_trend,
            "op_turnaround":  op_turnaround,
            "payout_ratio":   payout_ratio,
        })

    return pd.DataFrame(rows)


def apply_hard_filter(
    stock_df: pd.DataFrame,
    fins_metrics: pd.DataFrame,
    mode: str = "growth",
) -> pd.DataFrame:
    """ハードフィルタ通過銘柄を返す。mode='growth' or 'value'"""
    df = stock_df.merge(fins_metrics, on="code", how="left")

    # 時価総額計算
    df["market_cap"] = df["close"] * df["sh_out"].fillna(0)

    if mode == "value":
        mask = (
            (df["PBR"].fillna(999)         <= HARD_VALUE["pbr_max"])           &
            (df["PER"].fillna(999)         <= HARD_VALUE["per_max"])           &
            (df["PER"].fillna(0)           >  0)                               &
            (df["ROE"].fillna(0)           >= HARD_VALUE["roe_min"])           &
            (df["equity_ratio"].fillna(0)  >= HARD_VALUE["equity_ratio_min"])  &
            (df["market_cap"]              >= HARD_VALUE["market_cap_min"])    &
            (df["rev_growth"].fillna(-999) >= HARD_VALUE["rev_growth_min"])    &  # 売上成長+3%（死に株排除）
            (df["op_positive"].fillna(False) == True)
        )
    else:
        mask = (
            (df["rev_growth"]    > HARD["rev_growth_min"])    &
            (df["profit_growth"] > HARD["profit_growth_min"]) &
            (df["ROE"]           > HARD["roe_min"])           &
            (df["equity_ratio"]  > HARD["equity_ratio_min"])  &
            (df["market_cap"]    > HARD["market_cap_min"])    &
            (df["op_positive"].fillna(False) == True)          # 営業黒字（Altman Z補完）
        )
    return df[mask].copy()


# ════════════════════════════════════════════════════════════════════════
#  ② ファンダスコア（パーセンタイル正規化）
# ════════════════════════════════════════════════════════════════════════

def _percentile(series: pd.Series, invert: bool = False) -> pd.Series:
    """パーセンタイル順位（0〜100）。NaN は 50 で補完。"""
    ranked = series.rank(pct=True, na_option="keep") * 100
    if invert:
        ranked = 100 - ranked
    return ranked.fillna(50.0)


def calc_funda_score(df: pd.DataFrame, mode: str = "growth") -> pd.DataFrame:
    """
    ファンダスコアを計算して df に列追加して返す。
    mode='growth': Growth50 + Quality25 + Value25
    mode='value' : Value50 + Quality25 + Growth25
    PSR = market_cap / sales_fy
    """
    df = df.copy()
    funda_max   = FUNDA_MAX_VALUE   if mode == "value" else FUNDA_MAX
    val_metrics = VALUE_METRICS_VALUE if mode == "value" else VALUE_METRICS

    # PSR
    df["psr"] = np.where(
        (df["sales_fy"] > 0) & df["market_cap"].notna(),
        df["market_cap"] / df["sales_fy"],
        np.nan,
    )

    score = pd.Series(0.0, index=df.index)
    for metric, max_pt in funda_max.items():
        invert = (metric in val_metrics)
        col_map = {
            "rev_growth":    "rev_growth",
            "profit_growth": "profit_growth",
            "eps_growth":    "eps_growth",
            "roe":           "ROE",
            "op_margin":     "op_margin",
            "equity_ratio":  "equity_ratio",
            "per":           "PER",
            "psr":           "psr",
            "pbr":           "PBR",
        }
        col = col_map[metric]
        if col not in df.columns:
            continue
        pct = _percentile(df[col], invert=invert)
        score += pct / 100.0 * max_pt

    if mode == "value":
        # 増配トレンドボーナス（+5pt/1期, +10pt/2期連続）
        if "div_trend" in df.columns:
            score += df["div_trend"].fillna(0).clip(0, 2) * 5.0

        # 営業利益トレンドボーナス（+5pt/1期増, +10pt/2期連続増）
        if "op_trend" in df.columns:
            score += df["op_trend"].fillna(0).clip(0, 2) * 5.0

        # V字転換ボーナス（前期減益→今期回復: +15pt）
        # 2期連続増益（+10pt）より重く評価 = 最も上昇しやすいゾーン
        if "op_turnaround" in df.columns:
            score += df["op_turnaround"].fillna(False).astype(float) * 15.0

        # 配当性向ボーナス（上限70%のみ: +5pt）
        # 下限なし → 低配当でも内部留保中の優良企業を排除しない
        if "payout_ratio" in df.columns:
            pr = df["payout_ratio"].fillna(-1)
            score += ((pr > 0) & (pr <= 70)).astype(float) * 5.0

    df["funda_score"] = score.round(2)
    return df


# ════════════════════════════════════════════════════════════════════════
#  ③ テクニカルスコア
# ════════════════════════════════════════════════════════════════════════

def _calc_rsi(close: pd.Series, period: int = 14) -> float:
    delta = close.diff().dropna()
    gain  = delta.clip(lower=0)
    loss  = (-delta).clip(lower=0)
    avg_g = gain.rolling(period).mean().iloc[-1]
    avg_l = loss.rolling(period).mean().iloc[-1]
    if pd.isna(avg_g) or pd.isna(avg_l):
        return np.nan
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return round(100 - 100 / (1 + rs), 2)


def _calc_macd(close: pd.Series):
    """(macd, signal, prev_macd, prev_signal) を返す。"""
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd  = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    if len(macd) < 2:
        return np.nan, np.nan, np.nan, np.nan
    return (
        float(macd.iloc[-1]),
        float(signal.iloc[-1]),
        float(macd.iloc[-2]),
        float(signal.iloc[-2]),
    )


def _tech_score_single(cp: pd.DataFrame, mode: str = "growth") -> dict:
    """1銘柄分のテクニカルスコアを計算。mode='growth' or 'value'"""
    cp = cp.sort_values("Date").reset_index(drop=True)

    # 株式分割対応: AdjFactorで過去価格を現在スケールに正規化
    raw_close  = pd.to_numeric(cp["AdjC"],      errors="coerce")
    adj_factor = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
    rev_cumprod = adj_factor.iloc[::-1].cumprod().iloc[::-1]
    cum_factor  = rev_cumprod.shift(-1).fillna(1.0)
    close = (raw_close * cum_factor).dropna()

    vol = pd.to_numeric(cp["AdjVo"], errors="coerce").fillna(0)

    if len(close) < 26:
        return {"tech_score": np.nan, "tech_detail": {}}

    latest_close = float(close.iloc[-1])
    latest_vol   = float(vol.iloc[-1])

    # 移動平均
    ma25       = float(close.rolling(25).mean().iloc[-1])  if len(close) >= 25  else np.nan
    ma60       = float(close.rolling(60).mean().iloc[-1])  if len(close) >= 60  else np.nan
    ma200      = float(close.rolling(200).mean().iloc[-1]) if len(close) >= 200 else np.nan
    ma25_prev5 = float(close.rolling(25).mean().iloc[-6])  if len(close) >= 30  else np.nan

    # 出来高移動平均（バリューモード用: 5日平均/20日平均でトレンドを評価）
    vol_avg5  = float(vol.rolling(5).mean().iloc[-1])  if len(vol) >= 5  else float(vol.mean())
    vol_avg20 = float(vol.rolling(20).mean().iloc[-1]) if len(vol) >= 20 else float(vol.mean())

    # RSI
    rsi = _calc_rsi(close)

    # MACD
    macd, sig, macd_p, sig_p = _calc_macd(close)

    # 出来高 25日平均
    vol_avg25 = float(vol.rolling(25).mean().iloc[-1]) if len(vol) >= 25 else float(vol.mean())

    # 高値ブレイク
    high20 = float(pd.to_numeric(cp["AdjH"], errors="coerce").tail(20).max()) if "AdjH" in cp else np.nan
    high60 = float(pd.to_numeric(cp["AdjH"], errors="coerce").tail(60).max()) if "AdjH" in cp else np.nan

    score = 0

    # ── MA スコア（30点）
    ma_score  = 0
    ma200_dev = np.nan   # MA200乖離率（%）
    if mode == "value":
        # バリュー株: MA200乖離率で精度を上げる
        # MA200をちょうど上抜けた（0〜5%）が最高エントリー、遠いほど"出遅れ"
        if pd.notna(ma200) and ma200 > 0:
            ma200_dev = (latest_close - ma200) / ma200 * 100
            if ma200_dev >= 0:         # MA200上（長期上昇転換済み）
                if ma200_dev <= 5:
                    ma_score = 30      # MA200をちょうど上抜け（最高）
                elif ma200_dev <= 15:
                    ma_score = 20      # 上抜けて間もない（良好）
                else:
                    ma_score = 10      # すでに大きく上昇（出遅れ気味）
            elif ma200_dev >= -5:      # MA200直下（上抜け直前）
                ma_score = 15          # 初動狙い：上抜け直前ゾーン
            else:                      # MA200から5%超下（長期下落継続）
                ma_score = 0
        elif pd.notna(ma25):         # MA200が算出不能な場合はMA25で代替
            if latest_close > ma25:
                ma_score = 15
    else:
        # 成長株: 短中期トレンド継続を重視
        if pd.notna(ma25) and pd.notna(ma60):
            if latest_close > ma25 and ma25 > ma60:
                ma_score = 30
            elif latest_close > ma25 and pd.notna(ma25_prev5) and ma25 > ma25_prev5:
                ma_score = 20
            elif latest_close > ma60:
                ma_score = 10
        elif pd.notna(ma25):
            if latest_close > ma25:
                ma_score = 15
    score += ma_score

    # ── RSI スコア（20点）
    rsi_score = 0
    if pd.notna(rsi):
        if mode == "value":
            # バリュー株: 底値圏からの反転（RSI 30-45 が最適エントリーゾーン）
            if 30 <= rsi <= 45:
                rsi_score = 20
            elif 45 < rsi <= 55:    # やや高め、まだ一定評価
                rsi_score = 10
            elif 25 <= rsi < 30:    # 過売り気味、慎重
                rsi_score = 5
        else:
            # 成長株: トレンド継続（RSI 50-65 が最適）
            if 50 <= rsi <= 65:
                rsi_score = 20
            elif 40 <= rsi < 50:
                rsi_score = 10
            elif 65 < rsi <= 75:
                rsi_score = 5
    score += rsi_score

    # ── MACD スコア（20点）
    macd_score = 0
    if pd.notna(macd) and pd.notna(sig):
        golden_cross = (pd.notna(macd_p) and pd.notna(sig_p)
                        and macd_p < sig_p and macd >= sig)
        if macd > 0 and macd > sig:
            macd_score = 20
        elif golden_cross:
            macd_score = 15
        elif macd > sig:
            macd_score = 10
    score += macd_score

    # ── 出来高スコア（15点）
    vol_score = 0
    if mode == "value":
        # バリュー株: 5日平均 ÷ 20日平均でじわじわ買い集められているかを評価
        if vol_avg20 > 0:
            vol_trend_ratio = vol_avg5 / vol_avg20
            if vol_trend_ratio >= 1.3:
                vol_score = 15   # 出来高増加トレンド（明確な需給変化）
            elif vol_trend_ratio >= 1.0:
                vol_score = 10   # 横ばい〜微増（静かな買い集め）
            else:
                vol_score = 0    # 出来高減少（誰も買っていない）
    else:
        # 成長株: 当日出来高 ÷ 25日平均（ブレイク日の急増を評価）
        if vol_avg25 > 0:
            ratio = latest_vol / vol_avg25
            if ratio >= 1.5:
                vol_score = 15
            elif ratio >= 1.0:
                vol_score = 10
    score += vol_score

    # ── 高値ブレイクスコア（15点）
    break_score = 0
    if pd.notna(high60) and latest_close >= high60:
        break_score = 15
    elif pd.notna(high20) and latest_close >= high20:
        break_score = 10
    score += break_score

    detail = {
        "ma25":     round(ma25, 1)     if pd.notna(ma25)     else None,
        "ma60":     round(ma60, 1)     if pd.notna(ma60)     else None,
        "ma200":    round(ma200, 1)    if pd.notna(ma200)    else None,
        "ma200_dev": round(ma200_dev, 1) if pd.notna(ma200_dev) else None,
        "rsi":      round(rsi, 1)      if pd.notna(rsi)      else None,
        "macd": round(macd, 4)    if pd.notna(macd) else None,
        "macd_signal": round(sig, 4) if pd.notna(sig) else None,
        "vol_ratio":       round(latest_vol / vol_avg25, 2) if vol_avg25 > 0 else None,
        "vol_trend_ratio": round(vol_avg5 / vol_avg20, 2)  if vol_avg20 > 0 else None,
        "ma_score": ma_score, "rsi_score": rsi_score,
        "macd_score": macd_score, "vol_score": vol_score,
        "break_score": break_score,
    }
    return {"tech_score": float(score), "tech_detail": detail}


def calc_tech_scores(
    filtered_df: pd.DataFrame,
    prices_df: pd.DataFrame,
    mode: str = "growth",
) -> pd.DataFrame:
    """filtered_df の各銘柄のテクニカルスコアを計算して列追加。"""
    results = []
    for _, row in filtered_df.iterrows():
        code = row["code"]   # 5桁コード
        cp   = prices_df[prices_df["Code"] == code]
        res  = _tech_score_single(cp, mode=mode) if len(cp) >= 26 else {"tech_score": np.nan, "tech_detail": {}}
        res["code"] = code
        results.append(res)

    tech_df = pd.DataFrame(results)[["code", "tech_score", "tech_detail"]]
    return filtered_df.merge(tech_df, on="code", how="left")


# ════════════════════════════════════════════════════════════════════════
#  最終スコア計算
# ════════════════════════════════════════════════════════════════════════

def calc_total_score(df: pd.DataFrame) -> pd.DataFrame:
    """Total = Funda × 0.60 + Technical × 0.40"""
    df = df.copy()
    f = df["funda_score"].fillna(0)
    t = df["tech_score"].fillna(0)
    df["total_score"] = (f * 0.60 + t * 0.40).round(2)
    return df.sort_values("total_score", ascending=False)


# ════════════════════════════════════════════════════════════════════════
#  売買シグナル
# ════════════════════════════════════════════════════════════════════════

# シグナル判定ルール（成長株モード）
_BUY_TOTAL_MIN   = 60
_BUY_TECH_MIN    = 60
_BUY_RSI_MIN     = 50
_BUY_RSI_MAX     = 65
_WATCH_TOTAL_MIN = 50
_STOP_LOSS_PCT   = 0.15   # -15%
_TARGET_PCT      = 0.25   # +25%

# シグナル判定ルール（バリュー株モード）
_BUY_RSI_MIN_VALUE = 30   # 底値圏からの反転エントリー
_BUY_RSI_MAX_VALUE = 45   # 30〜45 に絞る（落ちるナイフ対策）


def calc_trade_signals(df: pd.DataFrame, mode: str = "growth") -> pd.DataFrame:
    """
    各銘柄に売買シグナルとエントリー・損切り・利確価格を付与する。

    追加列:
      signal        : "BUY" / "WATCH" / "AVOID"
      signal_reason : 判定根拠の説明
      entry_breakout: ブレイクアウトエントリー価格
      entry_pullback: 押し目エントリー価格（MA25水準）
      stop_loss     : 損切り価格（-8% or MA25 の高い方）
      stop_pct      : 損切り幅（%）
      target        : 利確目標価格（+25%）
      target_pct    : 利益目標（%）
    """
    df = df.copy()
    signals, reasons = [], []
    entries_b, entries_p, stops, stop_pcts, targets = [], [], [], [], []

    # モード別 RSI 範囲
    rsi_min = _BUY_RSI_MIN_VALUE if mode == "value" else _BUY_RSI_MIN
    rsi_max = _BUY_RSI_MAX_VALUE if mode == "value" else _BUY_RSI_MAX

    for _, row in df.iterrows():
        close      = float(row["close"])
        total      = float(row.get("total_score", 0) or 0)
        tech       = float(row.get("tech_score",  0) or 0)
        detail     = row.get("tech_detail", {}) or {}
        rsi        = detail.get("rsi")
        ma25       = detail.get("ma25")

        # ── シグナル判定 ──────────────────────────────
        above_ma25 = (ma25 is not None) and (close > ma25)
        rsi_ok     = (rsi  is not None) and (rsi_min <= rsi <= rsi_max)

        if (total >= _BUY_TOTAL_MIN and tech >= _BUY_TECH_MIN
                and above_ma25 and rsi_ok):
            sig = "BUY"
            parts = []
            if not above_ma25: parts.append("MA25下")
            if not rsi_ok:     parts.append(f"RSI={rsi:.0f}" if rsi else "RSI欠損")
            reason = "全条件クリア"
        elif total >= _WATCH_TOTAL_MIN:
            sig = "WATCH"
            parts = []
            if total < _BUY_TOTAL_MIN:    parts.append(f"総合{total:.0f}<60")
            if tech  < _BUY_TECH_MIN:     parts.append(f"テクニカル{tech:.0f}<60")
            if not above_ma25:            parts.append("MA25下")
            if not rsi_ok:                parts.append(f"RSI={rsi:.0f}(対象:{rsi_min}-{rsi_max})" if rsi else "RSI範囲外")
            reason = " / ".join(parts) if parts else "スコア基準は満たすが条件不足"
        else:
            sig    = "AVOID"
            reason = f"総合スコア{total:.0f}<50"

        # ── 価格計算 ──────────────────────────────────
        entry_b = round(close * 1.005)          # ブレイクアウト（0.5%上）
        # 押し目エントリー: MA25 < close の時のみ意味がある
        entry_p = round(ma25) if (ma25 and close > ma25) else None
        # 損切り: close > MA25 なら max(-8%, MA25)。MA25下なら単純-8%
        if ma25 and close > ma25:
            stop = round(max(close * (1 - _STOP_LOSS_PCT), ma25))
        else:
            stop = round(close * (1 - _STOP_LOSS_PCT))
        stop_p  = round((stop - close) / close * 100, 1)
        target  = round(close * (1 + _TARGET_PCT))

        signals.append(sig)
        reasons.append(reason)
        entries_b.append(entry_b)
        entries_p.append(entry_p)
        stops.append(stop)
        stop_pcts.append(stop_p)
        targets.append(target)

    df["signal"]         = signals
    df["signal_reason"]  = reasons
    df["entry_breakout"] = entries_b
    df["entry_pullback"] = entries_p
    df["stop_loss"]      = stops
    df["stop_pct"]       = stop_pcts
    df["target"]         = targets
    df["target_pct"]     = _TARGET_PCT * 100

    # BUY → WATCH → AVOID の順に並べ替え（同一シグナル内はtotal_score順を維持）
    order = {"BUY": 0, "WATCH": 1, "AVOID": 2}
    df["_sig_order"] = df["signal"].map(order)
    df = df.sort_values(["_sig_order", "total_score"],
                        ascending=[True, False]).drop(columns="_sig_order")
    return df.reset_index(drop=True)


# ════════════════════════════════════════════════════════════════════════
#  地合いフィルター
# ════════════════════════════════════════════════════════════════════════

_MARKET_ETF = {
    "topix":     "13060",   # TOPIX連動ETF (1306)
    "growth250": "25160",   # グロース250連動ETF (2516)
}


def calc_market_condition(prices_df: pd.DataFrame) -> dict:
    """
    TOPIX ETFとグロース250 ETFのMA25を基に地合いを判定する。

    Returns:
        {
          "topix_close": float, "topix_ma25": float, "topix_above": bool,
          "growth_close": float, "growth_ma25": float, "growth_above": bool,
          "state": "強気" | "中立" | "弱気",
          "investment_level": 1.0 | 0.5 | 0.0,
          "comment": str,
        }
    """
    result = {}

    for key, code in _MARKET_ETF.items():
        cp = prices_df[prices_df["Code"] == code].copy()
        cp = cp.sort_values("Date").reset_index(drop=True)

        # 株式分割対応: AdjFactorで過去価格を現在スケールに正規化
        # AdjFactor[i] の後方累積積を shift(-1) して各行に乗算
        raw_close  = pd.to_numeric(cp["C"],         errors="coerce")
        adj_factor = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
        rev_cumprod = adj_factor.iloc[::-1].cumprod().iloc[::-1]
        cum_factor  = rev_cumprod.shift(-1).fillna(1.0)
        close_s = (raw_close * cum_factor).dropna()

        if close_s.empty:
            close_s = pd.to_numeric(cp["AdjC"], errors="coerce").dropna()

        if len(close_s) < 25:
            result[key] = {"close": None, "ma25": None, "above": None}
            continue

        latest = float(close_s.iloc[-1])
        ma25   = float(close_s.rolling(25).mean().iloc[-1])
        result[key] = {
            "close": round(latest, 1),
            "ma25":  round(ma25, 1),
            "above": latest > ma25,
        }

    topix_above  = result.get("topix",     {}).get("above")
    growth_above = result.get("growth250", {}).get("above")

    both_above    = topix_above is True  and growth_above is True
    both_below    = topix_above is False and growth_above is False

    if both_above:
        state, level, comment = "強気", 1.0, "全指数がMA25上。通常投資可。"
    elif both_below:
        state, level, comment = "弱気", 0.0, "全指数がMA25下。新規買い停止推奨。"
    else:
        state, level, comment = "中立", 0.5, "指数が混在。投資額を半分に抑制推奨。"

    return {
        "topix_close":   result.get("topix",     {}).get("close"),
        "topix_ma25":    result.get("topix",     {}).get("ma25"),
        "topix_above":   topix_above,
        "growth_close":  result.get("growth250", {}).get("close"),
        "growth_ma25":   result.get("growth250", {}).get("ma25"),
        "growth_above":  growth_above,
        "state":         state,
        "investment_level": level,
        "comment":       comment,
    }


# ════════════════════════════════════════════════════════════════════════
#  Claude API 分析
# ════════════════════════════════════════════════════════════════════════

_SYSTEM_PROMPT = """あなたは日本株の成長株専門アナリストです。
提供されたスクリーニングデータを基に、各銘柄の投資分析を行ってください。

## 出力形式（JSON）
以下のJSONフォーマットで返してください。
**重要: 各フィールドの値は必ず1行で記述してください。改行を含めないでください。**

{
  "market_comment": "現在の市場環境コメント（2〜3文）",
  "stocks": [
    {
      "code": "銘柄コード",
      "story": "成長ストーリー（なぜ上がるか・2〜3文）",
      "strength": "強み（1〜2文）",
      "risk": "主要リスク（1〜2文）",
      "catalyst": "次のカタリスト（決算・材料・業界動向）",
      "judgment": "買い候補 | 監視 | 見送り",
      "upside": "想定上昇余地（例: +30〜50%）"
    }
  ],
  "top3": ["銘柄コード1", "銘柄コード2", "銘柄コード3"],
  "top3_reason": "ベスト3の選定理由（3〜4文）"
}"""

_SYSTEM_PROMPT_VALUE = """あなたは日本株のバリュー投資専門アナリストです。
グレアム・バフェット流の割安株投資の観点から、各銘柄の投資分析を行ってください。

バリュートラップ（安いが上がらない銘柄）と本物の割安株を見分けることが重要です。

## 出力形式（JSON）
以下のJSONフォーマットで返してください。
**重要: 各フィールドの値は必ず1行で記述してください。改行を含めないでください。**

{
  "market_comment": "現在の市場環境コメント（2〜3文、バリュー投資視点）",
  "stocks": [
    {
      "code": "銘柄コード",
      "story": "割安の理由と株価回復シナリオ（なぜ市場に見落とされているか・2〜3文）",
      "strength": "割安の根拠・財務的強み（1〜2文）",
      "risk": "バリュートラップの可能性・主要リスク（1〜2文）",
      "catalyst": "株価を動かすカタリスト（PBR改善施策・増配・自社株買い・業績回復）",
      "judgment": "買い候補 | 監視 | 見送り",
      "upside": "想定上昇余地（例: +30〜50%）"
    }
  ],
  "top3": ["銘柄コード1", "銘柄コード2", "銘柄コード3"],
  "top3_reason": "ベスト3の選定理由（3〜4文、割安の質とカタリストを重視）"
}"""


def _build_prompt(top10: pd.DataFrame, mode: str = "growth") -> str:
    mode_label = "バリュー株" if mode == "value" else "成長株"
    lines = [
        f"# {mode_label}スクリーニング結果 Top10（{datetime.now().strftime('%Y-%m-%d')}）",
        "",
        "以下の銘柄を分析してください。",
        "",
        "| # | コード | 会社名 | 株価 | 時価総額(億) | 売上成長% | 利益成長% | ROE% | PER | PBR | ファンダ | テクニカル | 総合 |",
        "|---|--------|--------|------|-------------|---------|---------|------|-----|-----|--------|---------|------|",
    ]
    for i, row in enumerate(top10.itertuples(), 1):
        cap = f"{row.market_cap/1e8:.0f}" if pd.notna(row.market_cap) and row.market_cap > 0 else "N/A"
        lines.append(
            f"| {i} | {row.code_4} | {row.company_name} | {row.close:.0f} | {cap} "
            f"| {row.rev_growth:.1f} | {row.profit_growth:.1f} | {row.ROE:.1f} "
            f"| {row.PER:.1f} | {row.PBR:.1f} "
            f"| {row.funda_score:.1f} | {row.tech_score:.1f} | {row.total_score:.1f} |"
        )
    lines += [
        "",
        "各銘柄について投資分析を行い、指定のJSONフォーマットで返してください。",
        "数値だけでなく、なぜ株価が上がるか・なぜ上がらないかのストーリーを重視してください。",
    ]
    return "\n".join(lines)


def _repair_json_strings(s: str) -> str:
    """
    JSON文字列値の中に含まれる生の改行・タブを
    エスケープシーケンスに置換する（文字ベース処理）。
    """
    result  = []
    in_str  = False
    escaped = False
    for ch in s:
        if escaped:
            result.append(ch)
            escaped = False
        elif ch == "\\" and in_str:
            result.append(ch)
            escaped = True
        elif ch == '"':
            in_str = not in_str
            result.append(ch)
        elif in_str and ch == "\n":
            result.append("\\n")
        elif in_str and ch == "\r":
            pass   # CR は捨てる
        elif in_str and ch == "\t":
            result.append("\\t")
        else:
            result.append(ch)
    return "".join(result)


def _extract_json(text: str) -> dict:
    """
    Claude レスポンスから JSON を抽出してパース。
    json-repair で構造エラー・文字列内改行・カンマ欠落をすべて修復。
    """
    from json_repair import repair_json

    # ① マークダウンコードブロックを除去
    text = re.sub(r"```(?:json)?\s*", "", text)
    text = text.replace("```", "").strip()

    # ② 最外の { } を抽出
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if not match:
        raise ValueError(f"JSONが見つかりません: {text[:300]}")
    json_str = match.group()

    # ③ 文字列値内の生改行をエスケープ（repair_json の前処理）
    json_str = _repair_json_strings(json_str)

    # ④ json-repair で残りの構造エラーを修復してパース
    result = repair_json(json_str, return_objects=True)
    if not isinstance(result, dict):
        raise ValueError(f"JSON修復後も辞書型になりません: {type(result)}")
    return result


def analyze_with_claude(top10: pd.DataFrame, mode: str = "growth") -> dict:
    """上位10銘柄をClaude APIに分析させる。"""
    key = os.getenv("ANTHROPIC_API_KEY")
    if not key:
        raise EnvironmentError(".env に ANTHROPIC_API_KEY が設定されていません")

    client = anthropic.Anthropic(api_key=key)
    prompt        = _build_prompt(top10, mode=mode)
    system_prompt = _SYSTEM_PROMPT_VALUE if mode == "value" else _SYSTEM_PROMPT

    resp = client.messages.create(
        model=MODEL,
        max_tokens=8192,
        system=system_prompt,
        messages=[{"role": "user", "content": prompt}],
    )
    text = resp.content[0].text.strip()

    result = _extract_json(text)
    result["input_tokens"]  = resp.usage.input_tokens
    result["output_tokens"] = resp.usage.output_tokens
    result["cost_usd"] = (
        resp.usage.input_tokens  * INPUT_COST_PER_TOKEN +
        resp.usage.output_tokens * OUTPUT_COST_PER_TOKEN
    )
    return result


# ════════════════════════════════════════════════════════════════════════
#  パイプライン実行（メインエントリ）
# ════════════════════════════════════════════════════════════════════════

def run_pipeline(use_claude: bool = True, progress_callback=None, mode: str = "growth") -> dict:
    """
    パイプライン全体を実行して結果辞書を返す。

    Returns:
        {
            "filtered":    DataFrame（ハードフィルタ通過銘柄）,
            "scored":      DataFrame（スコア計算済み全銘柄）,
            "top10":       DataFrame（上位10銘柄）,
            "ai_analysis": dict（Claude API 分析結果）or None,
            "stats": {
                "total": int,
                "filtered": int,
                "top10": int,
            }
        }
    """
    def _cb(msg):
        if progress_callback:
            progress_callback(msg)

    _cb("データ読み込み中...")
    stock_df  = _load_stock_cache()
    prices_df = _load_prices()
    fins_fy   = _load_fins_fy()

    _cb("財務指標を計算中...")
    fins_metrics = _build_fins_metrics(fins_fy, prices_df)

    _cb("① ハードフィルタ適用中...")
    filtered = apply_hard_filter(stock_df, fins_metrics, mode=mode)

    if filtered.empty:
        return {
            "filtered": filtered, "scored": filtered,
            "top10": filtered, "ai_analysis": None,
            "mode": mode,
            "stats": {"total": len(stock_df), "filtered": 0, "top10": 0},
        }

    _cb("② ファンダスコア計算中...")
    scored = calc_funda_score(filtered, mode=mode)

    _cb("③ テクニカルスコア計算中...")
    scored = calc_tech_scores(scored, prices_df, mode=mode)

    _cb("最終スコア合算中...")
    scored = calc_total_score(scored)

    _cb("売買シグナル計算中...")
    scored = calc_trade_signals(scored, mode=mode)

    _cb("地合いフィルター計算中...")
    market = calc_market_condition(prices_df)

    top10 = scored.head(10).reset_index(drop=True)

    ai_result = None
    if use_claude:
        _cb("Claude API で投資分析中...")
        try:
            ai_result = analyze_with_claude(top10, mode=mode)
        except Exception as e:
            ai_result = {"error": str(e)}

    _cb("完了")
    return {
        "filtered":         filtered,
        "scored":           scored,
        "top10":            top10,
        "ai_analysis":      ai_result,
        "market_condition": market,
        "mode":             mode,
        "stats": {
            "total":    len(stock_df),
            "filtered": len(filtered),
            "top10":    len(top10),
        },
    }
