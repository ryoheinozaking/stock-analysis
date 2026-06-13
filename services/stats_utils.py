# -*- coding: utf-8 -*-
"""
統計ユーティリティ（診断サービス共有）。

Rank IC 診断で使う Spearman 順位相関を1箇所に集約する。
diagnose_value_service / diagnose_growth_service が共有して使うことで
実装のドリフト（片方だけ直す）を防ぐ。
"""
import numpy as np
import pandas as pd


def spearmanr(x, y, min_samples: int = 5) -> float:
    """NaN を除外した Spearman 順位相関（タイ対応）。

    Spearman の定義どおり「順位に変換してからの Pearson 相関」で計算する。
    旧実装の簡易公式 ``1 - 6Σd²/(n(n²-1))`` は**同順位（タイ）が無い前提**で
    のみ正しく、SEPA ステージ（4値）や boolean シグナル（activist 等）のように
    タイの多い系列では |IC| を系統的に過大評価していた。順位の Pearson 相関は
    タイを正しく扱う（pandas ``Series.rank().corr()`` は average rank + Pearson）。

    Args:
        x, y: 数値配列（list / np.ndarray / pd.Series）。
        min_samples: 有効ペアがこの数未満なら NaN を返す。

    Returns:
        Spearman 相関係数。有効サンプル不足、または分散ゼロ（定数列）の場合は NaN。
    """
    x = np.asarray(x, dtype="float64")
    y = np.asarray(y, dtype="float64")
    mask = ~(np.isnan(x) | np.isnan(y))
    if mask.sum() < min_samples:
        return float("nan")
    xr = pd.Series(x[mask]).rank()
    yr = pd.Series(y[mask]).rank()
    # 定数列（分散ゼロ）は Spearman 未定義 → NaN。
    # 明示判定して numpy の 0除算 warning を避ける。
    if xr.nunique() < 2 or yr.nunique() < 2:
        return float("nan")
    # 既定は Pearson。順位に対する Pearson 相関 = タイ対応 Spearman。
    return float(xr.corr(yr))
