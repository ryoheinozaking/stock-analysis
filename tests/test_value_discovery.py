# -*- coding: utf-8 -*-
"""
バリュー深層分析動線「経営変化 × 出遅れ 発掘」の候補抽出ロジック回帰テスト。

select_value_discovery_candidates(scored) は
- 経営変化シグナル（activist OR div_trend>=1 OR payout_ratio in [25,70]）あり
- かつ RSI<=60（欠損は含める）
を total_score 降順で main、RSI>60 を reference として返す。
利益モメンタムのみ（op_trend）は候補に入らない。
"""
import numpy as np
import pandas as pd

from services.pipeline_service import select_value_discovery_candidates


def _row(code_4, total_score, *, activist=False, div_trend=0,
         payout_ratio=np.nan, rsi=40.0, op_trend=0):
    return {
        "code_4": code_4,
        "company_name": f"会社{code_4}",
        "total_score": total_score,
        "activist": activist,
        "div_trend": div_trend,
        "payout_ratio": payout_ratio,
        "rsi": rsi,
        "op_trend": op_trend,
    }


def _scored(rows):
    return pd.DataFrame(rows)


def test_activist_row_is_main_candidate():
    main, ref = select_value_discovery_candidates(
        _scored([_row("1000", 80, activist=True, rsi=40)]))
    assert list(main["code_4"]) == ["1000"]
    assert ref.empty


def test_div_trend_row_is_candidate():
    main, _ = select_value_discovery_candidates(
        _scored([_row("1001", 70, div_trend=1, rsi=45)]))
    assert list(main["code_4"]) == ["1001"]


def test_payout_in_band_is_candidate_out_of_band_is_not():
    main, ref = select_value_discovery_candidates(_scored([
        _row("1002", 70, payout_ratio=50, rsi=40),   # 50% → 候補
        _row("1003", 65, payout_ratio=80, rsi=40),   # 80% → 還元姿勢とみなさない
    ]))
    assert list(main["code_4"]) == ["1002"]
    assert ref.empty


def test_no_signal_row_excluded():
    main, ref = select_value_discovery_candidates(
        _scored([_row("1004", 90, rsi=40)]))   # シグナル無し
    assert main.empty and ref.empty


def test_profit_momentum_only_excluded():
    # op_trend のみ（経営の意志シグナル無し）は候補に入らない（思想の確認）
    main, ref = select_value_discovery_candidates(
        _scored([_row("1005", 90, op_trend=2, rsi=40)]))
    assert main.empty and ref.empty


def test_overheated_signal_goes_to_reference():
    main, ref = select_value_discovery_candidates(
        _scored([_row("1006", 80, activist=True, rsi=72)]))
    assert main.empty
    assert list(ref["code_4"]) == ["1006"]


def test_missing_rsi_is_included_in_main():
    main, _ = select_value_discovery_candidates(
        _scored([_row("1007", 80, activist=True, rsi=np.nan)]))
    assert list(main["code_4"]) == ["1007"]


def test_main_sorted_by_total_score_desc():
    main, _ = select_value_discovery_candidates(_scored([
        _row("1008", 70, activist=True, rsi=40),
        _row("1009", 85, activist=True, rsi=40),
        _row("1010", 78, div_trend=2, rsi=40),
    ]))
    assert list(main["code_4"]) == ["1009", "1010", "1008"]


def test_empty_input_returns_empty():
    main, ref = select_value_discovery_candidates(pd.DataFrame())
    assert main.empty and ref.empty
