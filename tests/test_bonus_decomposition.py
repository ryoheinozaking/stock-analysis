# -*- coding: utf-8 -*-
"""経営変化ボーナス成分の列露出テスト（funda_score 値は不変が最重要）。"""
import numpy as np
import pandas as pd

from services.pipeline_service import calc_funda_score


def _vrow(code_4, pbr, sales_fy, market_cap, **kw):
    row = {
        "code": code_4 + "0", "code_4": code_4,
        "PBR": pbr, "PER": 12.0, "ROE": 8.0, "op_margin": 5.0,
        "sales_fy": sales_fy, "market_cap": market_cap,
        "div_trend": 0, "op_trend": 0, "op_turnaround": False,
        "payout_ratio": np.nan,
    }
    row.update(kw)
    return row


def _score(rows):
    return calc_funda_score(pd.DataFrame(rows), mode="value")


def test_bonus_component_columns_exist():
    df = _score([_vrow("1000", 0.8, 1e11, 2e11, div_trend=2)])
    for col in ["bonus_activist", "bonus_div", "bonus_op",
                "bonus_turnaround", "bonus_payout", "bonus_total",
                "payout_in_band", "activist"]:
        assert col in df.columns


def test_bonus_total_excludes_turnaround_and_payout():
    # 2026-06-14 撤廃: V字・配当性向は funda_score に加算しない（列は監視用に残す）。
    # bonus_total = activist + div + op のみ。
    df = _score([_vrow("1000", 0.8, 1e11, 2e11,
                        div_trend=2, op_trend=1, op_turnaround=True,
                        payout_ratio=50.0)])
    r = df.iloc[0]
    assert r["bonus_turnaround"] == 15.0   # 列は監視用に計算される
    assert r["bonus_payout"] == 10.0
    assert r["bonus_div"] == 10.0
    assert r["bonus_op"] == 5.0
    # bonus_total は activist+div+op のみ（V字・配当性向は除外）
    assert r["bonus_total"] == r["bonus_activist"] + r["bonus_div"] + r["bonus_op"]


def test_component_points_match_definition():
    df = _score([
        _vrow("1000", 0.8, 1e11, 2e11, div_trend=2),            # +10 div
        _vrow("1001", 0.8, 1e11, 2e11, op_turnaround=True),     # +15 turnaround
        _vrow("1002", 0.8, 1e11, 2e11, payout_ratio=50.0),      # +10 payout(40-70)
        _vrow("1003", 0.8, 1e11, 2e11, payout_ratio=30.0),      # +5 payout(25-40)
        _vrow("1004", 0.8, 1e11, 2e11, payout_ratio=80.0),      # 0 payout(>70)
    ]).set_index("code_4")
    assert df.loc["1000", "bonus_div"] == 10.0
    assert df.loc["1001", "bonus_turnaround"] == 15.0
    assert df.loc["1002", "bonus_payout"] == 10.0
    assert df.loc["1002", "payout_in_band"] == 1.0
    assert df.loc["1003", "bonus_payout"] == 5.0
    assert df.loc["1004", "bonus_payout"] == 0.0
    assert df.loc["1004", "payout_in_band"] == 0.0


def test_funda_score_is_core_plus_bonus_total_only():
    # funda_score == コア percentile + bonus_total。コアが同一の行群では
    # funda_score の差は bonus_total の差だけになる（V字・配当性向は bonus_total に
    # 入らないので funda_score を動かさない）。
    rows = [
        _vrow("1000", 0.8, 1e11, 2e11),
        _vrow("1001", 0.8, 1e11, 2e11, op_turnaround=True, payout_ratio=50.0),
        _vrow("1002", 0.8, 1e11, 2e11, div_trend=2),
    ]
    df = _score(rows)
    cores = (df["funda_score"] - df["bonus_total"]).round(6)
    assert cores.nunique() == 1


def test_bonus_variant_subtraction():
    from services.diagnose_value_service import _add_bonus_variants
    scored = pd.DataFrame([{
        "funda_score": 100.0, "bonus_activist": 10.0, "bonus_div": 5.0,
        "bonus_op": 0.0, "bonus_turnaround": 15.0, "bonus_payout": 10.0,
        "bonus_total": 40.0,
    }])
    _add_bonus_variants([{"scored": scored, "as_of": "2024-01-31"}])
    r = scored.iloc[0]
    assert r["funda_no_activist"]   == 90.0
    assert r["funda_no_turnaround"] == 85.0
    assert r["funda_no_bonus"]      == 60.0
