# -*- coding: utf-8 -*-
"""
services.stats_utils.spearmanr の回帰テスト。

Spearman の定義は「順位に変換してからの Pearson 相関」。
旧 _spearmanr の簡易公式 1-6Σd²/(n(n²-1)) は同順位（タイ）が無い前提でのみ正しく、
SEPA ステージ（4値）や boolean シグナルのようなタイの多い系列では |IC| を過大評価していた。
"""
import numpy as np
import pandas as pd
import pytest

from services.stats_utils import spearmanr


def _d2_formula(x, y):
    """旧 _spearmanr（d² 簡易公式）。タイで歪む版を参照用に再現。"""
    x = np.asarray(x, dtype="float64")
    y = np.asarray(y, dtype="float64")
    m = ~(np.isnan(x) | np.isnan(y))
    x, y = x[m], y[m]
    n = len(x)
    xr = pd.Series(x).rank().values
    yr = pd.Series(y).rank().values
    d2 = float(np.sum((xr - yr) ** 2))
    denom = n * (n ** 2 - 1)
    return 1.0 - 6.0 * d2 / denom


def test_perfect_monotonic_is_one():
    assert spearmanr([1, 2, 3, 4, 5, 6], [2, 4, 6, 8, 10, 12]) == pytest.approx(1.0)


def test_perfect_inverse_is_minus_one():
    assert spearmanr([1, 2, 3, 4, 5, 6], [12, 10, 8, 6, 4, 2]) == pytest.approx(-1.0)


def test_equals_rank_pearson_continuous():
    rng = np.random.default_rng(1)
    x = rng.normal(size=200)
    y = 0.5 * x + rng.normal(size=200)
    expected = pd.Series(x).rank().corr(pd.Series(y).rank())
    assert spearmanr(x, y) == pytest.approx(expected)


def test_equals_rank_pearson_with_heavy_ties():
    # SEPA ステージ的（4値・大量タイ）でも定義（順位Pearson）と一致する
    rng = np.random.default_rng(2)
    ret = rng.normal(size=300)
    stage = np.clip(np.round(rng.integers(1, 5, 300) + 0.2 * ret), 1, 4).astype(float)
    expected = pd.Series(stage).rank().corr(pd.Series(ret).rank())
    assert spearmanr(stage, ret) == pytest.approx(expected)


def test_tie_heavy_differs_from_d2_formula():
    # boolean シグナル（タイ最大）では旧 d² 公式と明確に乖離する（バグの回帰ガード）
    rng = np.random.default_rng(3)
    ret = rng.normal(size=400)
    act = (rng.random(400) < 0.13).astype(float)
    ret2 = ret + 0.05 * act
    correct = spearmanr(act, ret2)
    buggy = _d2_formula(act, ret2)
    assert abs(correct - buggy) > 0.05  # 乖離する（バグが直っている証拠）
    assert correct == pytest.approx(pd.Series(act).rank().corr(pd.Series(ret2).rank()))


def test_nan_values_excluded():
    # 有効ペアは (1,2),(4,8),(5,10),(6,12) = 4 < 5 → NaN
    x = [1, 2, np.nan, 4, 5, 6]
    y = [2, np.nan, 6, 8, 10, 12]
    assert np.isnan(spearmanr(x, y))


def test_below_min_samples_returns_nan():
    assert np.isnan(spearmanr([1, 2, 3], [3, 2, 1]))


def test_constant_input_returns_nan():
    # 定数列は Spearman 未定義 → NaN（旧公式は誤った数値を返していた）
    assert np.isnan(spearmanr([5, 5, 5, 5, 5, 5], [1, 2, 3, 4, 5, 6]))
