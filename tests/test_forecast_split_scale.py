# -*- coding: utf-8 -*-
"""
予想 per-share 値（FEPS / FDivAnn）の分割スケール二重調整バグの回帰テスト

背景（2026-06-13 調査・6227 ＡＩメカテックで確認）:
  会社が分割（効力 2026-03-30, AdjFactor 0.3333）の発表後・効力前に開示した
  2Q 短信（2026-02-13）で、通期予想 EPS=163.93 を「分割後ベース」で開示していた。
  既存コードは「開示日 < 分割日 → 予想は分割前ベース」と一律仮定して
  split_factor(0.3333) を掛け、予想 PER を 143.7 倍（正: 47.9 倍）と 3 倍過大に算出。

  日付ルールだけでは pre/post を判定できない（会社により両ベースあり得る）ため、
  予想純利益 FNP ÷ 期末発行済株式数 ShOutFY との突合でスケールを判定する。
"""
import os

import numpy as np
import pandas as pd
import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# ════════════════════════════════════════════════════════════════════════
#  forecast_per_share_multiplier（純粋関数・スケール判定の核）
# ════════════════════════════════════════════════════════════════════════

def test_multiplier_post_split_disclosure_returns_one():
    """6227 型: FEPS が既に分割後ベース → 乗数 1.0（split_factor を掛けない）。

    FNP/shares = 3078M/6.283M ≈ 489.9（分割前 EPS 候補）、
    × split_factor(0.3333) ≈ 163.3（分割後 EPS 候補）。
    開示 FEPS=163.93 は後者に一致 → 既に最新スケール → 乗数 1.0。
    """
    from services.split_adjust import forecast_per_share_multiplier

    m = forecast_per_share_multiplier(
        per_share=163.93, aggregate=3_078_000_000.0,
        shares=6_283_000.0, split_factor=0.333333,
    )
    assert m == pytest.approx(1.0)


def test_multiplier_pre_split_disclosure_returns_split_factor():
    """分割前ベースで開示された予想 EPS → 乗数 = split_factor（前進調整が必要）。

    FNP/shares = 100（分割前候補）、開示 FEPS=100 が一致 → 分割前ベース →
    split_factor を掛けて最新スケールへ。
    """
    from services.split_adjust import forecast_per_share_multiplier

    m = forecast_per_share_multiplier(
        per_share=100.0, aggregate=100_000_000.0,
        shares=1_000_000.0, split_factor=0.5,
    )
    assert m == pytest.approx(0.5)


def test_multiplier_no_split_returns_one():
    """分割なし（split_factor==1.0）→ 判定不要で乗数 1.0。"""
    from services.split_adjust import forecast_per_share_multiplier

    m = forecast_per_share_multiplier(
        per_share=50.0, aggregate=50_000_000.0,
        shares=1_000_000.0, split_factor=1.0,
    )
    assert m == pytest.approx(1.0)


def test_multiplier_missing_aggregate_falls_back_to_split_factor():
    """FNP 欠損で判定不能 → 従来の日付ルール（分割前前提）にフォールバック。"""
    from services.split_adjust import forecast_per_share_multiplier

    m = forecast_per_share_multiplier(
        per_share=163.93, aggregate=np.nan,
        shares=6_283_000.0, split_factor=0.333333,
    )
    assert m == pytest.approx(0.333333)


def test_multiplier_missing_shares_falls_back_to_split_factor():
    """ShOutFY 欠損/ゼロで判定不能 → フォールバック。"""
    from services.split_adjust import forecast_per_share_multiplier

    assert forecast_per_share_multiplier(163.93, 3_078_000_000.0, np.nan, 0.333333) \
        == pytest.approx(0.333333)
    assert forecast_per_share_multiplier(163.93, 3_078_000_000.0, 0.0, 0.333333) \
        == pytest.approx(0.333333)


def test_multiplier_unreliable_anchor_falls_back_to_split_factor():
    """FNP/shares のアンカーが per_share とかけ離れている（連単不一致等）→
    判定信頼できず従来動作にフォールバック。"""
    from services.split_adjust import forecast_per_share_multiplier

    # cand_pre=100, cand_post=33。per_share=1000 は両者から 10x 以上乖離。
    m = forecast_per_share_multiplier(
        per_share=1000.0, aggregate=100_000_000.0,
        shares=1_000_000.0, split_factor=0.333333,
    )
    assert m == pytest.approx(0.333333)


def test_multiplier_none_per_share_returns_split_factor():
    """per_share が None/NaN → 乗数判定の対象外、split_factor を返す（呼び出し側で無視される）。"""
    from services.split_adjust import forecast_per_share_multiplier

    assert forecast_per_share_multiplier(None, 3_078_000_000.0, 6_283_000.0, 0.5) \
        == pytest.approx(0.5)
    assert forecast_per_share_multiplier(np.nan, 3_078_000_000.0, 6_283_000.0, 0.5) \
        == pytest.approx(0.5)


# ════════════════════════════════════════════════════════════════════════
#  deep_analysis_helper.compute_valuation_estimate（helper の予想 PER/利回り）
# ════════════════════════════════════════════════════════════════════════

def _prices_with_split(last_close, split_factor, split_date, last_date,
                       code="62270", n_before=30, n_after=10):
    """split_date に AdjFactor=split_factor を 1 本だけ持つ合成株価 df を作る。

    末尾 close を last_close に固定（normalize_close 末尾 = 生 C 末尾）。
    """
    pre_dates = pd.bdate_range(end=pd.Timestamp(split_date) - pd.Timedelta(days=1),
                               periods=n_before)
    post_dates = pd.bdate_range(start=split_date, periods=n_after)
    dates = list(pre_dates) + list(post_dates)
    af = [1.0] * len(pre_dates) + [split_factor] + [1.0] * (len(post_dates) - 1)
    c = [last_close] * len(dates)  # 末尾だけ意味があるので簡略化（全て同値）
    return pd.DataFrame({
        "Date": [d.strftime("%Y-%m-%d") for d in dates],
        "Code": code,
        "O": c, "H": c, "L": c, "C": c,
        "Vo": 100000.0,
        "AdjFactor": af,
    })


def test_helper_valuation_uses_post_split_forecast_eps():
    """6227 型: 最新 2Q の分割後 FEPS=163.93 で PER≈47.9（143.7 にならない）。
    予想配当 FDivAnn=17.0（分割後）で利回り≈0.22%（実績 45.0 を流用しない）。
    """
    from scripts.deep_analysis_helper import compute_valuation_estimate

    cp = _prices_with_split(last_close=7850.0, split_factor=0.333333,
                            split_date="2026-03-30", last_date="2026-06-12")
    fins = pd.DataFrame([
        # 最新: 2Q 短信（分割後ベースで通期予想を開示）
        {"Code": "62270", "DiscDate": "2026-02-13", "CurPerType": "2Q",
         "DocType": "2QFinancialStatements_Consolidated_JP", "CurFYEn": "2026-06-30",
         "FEPS": 163.93, "FNP": 3_078_000_000.0, "ShOutFY": 6_283_000.0,
         "FDivAnn": 17.0, "EPS": 298.4, "DivAnn": np.nan, "NxFEPS": np.nan},
        # 旧: FY 本決算（実績 EPS は分割前ベース、予想は NxFEPS 側）
        {"Code": "62270", "DiscDate": "2025-08-08", "CurPerType": "FY",
         "DocType": "FYFinancialStatements_Consolidated_JP", "CurFYEn": "2025-06-30",
         "FEPS": np.nan, "FNP": np.nan, "ShOutFY": 6_283_000.0,
         "FDivAnn": np.nan, "EPS": 54.62, "DivAnn": 45.0, "NxFEPS": 255.67},
    ])

    per, div_yield = compute_valuation_estimate(fins, last_close=7850.0, cp=cp)
    assert per == pytest.approx(47.9, abs=0.5)
    assert div_yield == pytest.approx(0.217, abs=0.02)


def test_helper_valuation_no_split_plain_per():
    """分割なしの通常銘柄: PER = close / FEPS をそのまま返す。"""
    from scripts.deep_analysis_helper import compute_valuation_estimate

    dates = pd.bdate_range(end="2026-06-12", periods=40)
    cp = pd.DataFrame({
        "Date": [d.strftime("%Y-%m-%d") for d in dates],
        "Code": "10000", "O": 2000.0, "H": 2000.0, "L": 2000.0, "C": 2000.0,
        "Vo": 100000.0, "AdjFactor": 1.0,
    })
    fins = pd.DataFrame([
        {"Code": "10000", "DiscDate": "2026-05-10", "CurPerType": "FY",
         "DocType": "FYFinancialStatements_Consolidated_JP", "CurFYEn": "2026-03-31",
         "FEPS": 100.0, "FNP": 100_000_000.0, "ShOutFY": 1_000_000.0,
         "FDivAnn": 40.0, "EPS": 95.0, "DivAnn": 38.0, "NxFEPS": np.nan},
    ])
    per, div_yield = compute_valuation_estimate(fins, last_close=2000.0, cp=cp)
    assert per == pytest.approx(20.0, abs=0.1)        # 2000 / 100
    assert div_yield == pytest.approx(2.0, abs=0.05)  # 40 / 2000


# ════════════════════════════════════════════════════════════════════════
#  batch_service._compute_metrics（stock_cache 側の PER）
# ════════════════════════════════════════════════════════════════════════

def test_compute_metrics_post_split_forecast_per_not_tripled():
    """6227 型: stock_cache の PER が 143.7 ではなく ≈47.9 になる。"""
    from services.batch_service import _compute_metrics

    cp = _prices_with_split(last_close=7850.0, split_factor=0.333333,
                            split_date="2026-03-30", last_date="2026-06-12")
    fins = pd.DataFrame([
        {"Code": "62270", "DiscDate": "2026-02-13", "CurPerType": "2Q",
         "DocType": "2QFinancialStatements_Consolidated_JP", "CurFYEn": "2026-06-30",
         "FEPS": 163.93, "FNP": 3_078_000_000.0, "FDivAnn": 17.0,
         "ShOutFY": 6_283_000.0, "EPS": 298.4, "NP": 1_864_000_000.0,
         "Eq": 11_000_000_000.0, "EqAR": 0.40, "DivAnn": np.nan, "NxFEPS": np.nan},
        {"Code": "62270", "DiscDate": "2025-08-08", "CurPerType": "FY",
         "DocType": "FYFinancialStatements_Consolidated_JP", "CurFYEn": "2025-06-30",
         "FEPS": np.nan, "FNP": np.nan, "FDivAnn": np.nan,
         "ShOutFY": 6_283_000.0, "EPS": 54.62, "NP": 337_000_000.0,
         "Eq": 11_000_000_000.0, "EqAR": 0.40, "DivAnn": 45.0, "NxFEPS": 255.67},
    ])
    fins["DiscDate"] = pd.to_datetime(fins["DiscDate"])

    row = _compute_metrics("62270", cp, fins, None)
    assert row is not None
    assert row["PER"] == pytest.approx(47.9, abs=1.0)


def test_compute_metrics_pre_split_forecast_per_keeps_adjustment():
    """分割前ベースで予想を開示した銘柄は従来どおり split_factor を適用。

    FEPS=300 が分割前ベース（FNP/shares=300 に一致）、split 0.5 が後に発生。
    最新スケール per_base = 300×0.5 = 150 → PER = 1500/150 = 10。
    （誤って二重調整を消すと 1500/300 = 5 になってしまう）
    """
    from services.batch_service import _compute_metrics

    cp = _prices_with_split(last_close=1500.0, split_factor=0.5,
                            split_date="2026-03-30", last_date="2026-06-12",
                            code="99990")
    fins = pd.DataFrame([
        {"Code": "99990", "DiscDate": "2026-02-13", "CurPerType": "2Q",
         "DocType": "2QFinancialStatements_Consolidated_JP", "CurFYEn": "2026-06-30",
         "FEPS": 300.0, "FNP": 300_000_000.0, "FDivAnn": np.nan,
         "ShOutFY": 1_000_000.0, "EPS": 150.0, "NP": 150_000_000.0,
         "Eq": 5_000_000_000.0, "EqAR": 0.5, "DivAnn": np.nan, "NxFEPS": np.nan},
        {"Code": "99990", "DiscDate": "2025-08-08", "CurPerType": "FY",
         "DocType": "FYFinancialStatements_Consolidated_JP", "CurFYEn": "2025-06-30",
         "FEPS": np.nan, "FNP": np.nan, "FDivAnn": np.nan,
         "ShOutFY": 1_000_000.0, "EPS": 120.0, "NP": 120_000_000.0,
         "Eq": 5_000_000_000.0, "EqAR": 0.5, "DivAnn": 30.0, "NxFEPS": 140.0},
    ])
    fins["DiscDate"] = pd.to_datetime(fins["DiscDate"])

    row = _compute_metrics("99990", cp, fins, None)
    assert row is not None
    assert row["PER"] == pytest.approx(10.0, abs=0.3)
