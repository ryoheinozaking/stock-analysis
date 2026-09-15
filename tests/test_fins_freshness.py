# -*- coding: utf-8 -*-
"""
fins_cache 取りこぼし・成長率の比較年度ずれ・変則決算期の回帰テスト（2026-09-15）

背景:
1. update_fins の差分更新が「当日の開示」だけを取得していたため、
   データ更新ボタンを押さなかった日の開示が永久に欠落していた
   （2026-04-16 以降、開示ゼロの営業日が約 90 日。7803/5254/4417 の本決算が未収録）。
2. _compute_metrics の成長率が「予想値 ÷ 最新確定FYのさらに1期前の実績」になっており、
   2 年分の伸びを 1 年の成長率として出していた（7803 利益成長 +586% など）。
3. 決算期変更（例: 325A の 7ヶ月決算）で期間長の異なる FY を素朴に比較していた。
"""
import json

import numpy as np
import pandas as pd
import pytest

_STMT = "FYFinancialStatements_Consolidated_JP"
_Q2 = "2QFinancialStatements_Consolidated_JP"
_Q1 = "1QFinancialStatements_Consolidated_JP"
_EARN_REV = "EarnForecastRevision"

_TS = pd.Timestamp


# ════════════════════════════════════════════════════════════════════════
#  合成データヘルパー
# ════════════════════════════════════════════════════════════════════════

def _row(code, disc_date, doctype, fy_st, fy_en, per_type="FY", **vals):
    row = {
        "Code": code, "DiscDate": disc_date, "DocType": doctype,
        "CurPerType": per_type, "CurFYSt": fy_st, "CurFYEn": fy_en,
        "NxtFYSt": np.nan, "NxtFYEn": np.nan,
        "Sales": np.nan, "OP": np.nan, "NP": np.nan, "EPS": np.nan,
        "Eq": np.nan, "EqAR": np.nan, "TA": np.nan, "CFO": np.nan,
        "ShOutFY": np.nan, "DivAnn": np.nan,
        "FSales": np.nan, "FNP": np.nan, "FEPS": np.nan, "FDivAnn": np.nan,
        "NxFSales": np.nan, "NxFNp": np.nan, "NxFEPS": np.nan, "NxFDivAnn": np.nan,
    }
    row.update(vals)
    return row


def _df(rows):
    df = pd.DataFrame(rows)
    df["DiscDate"] = pd.to_datetime(df["DiscDate"])
    return df.sort_values(["Code", "DiscDate"], ascending=[True, False]).reset_index(drop=True)


def _prices(code, n=40, end="2026-09-15", base=1000.0):
    dates = pd.bdate_range(end=end, periods=n)
    return pd.DataFrame({
        "Date": dates.strftime("%Y-%m-%d"), "Code": code,
        "O": base, "H": base, "L": base, "C": base, "Vo": 100000.0, "AdjFactor": 1.0,
    })


_BS = dict(Eq=10_000.0, EqAR=0.5, TA=20_000.0, CFO=100.0, ShOutFY=1_000_000.0)


# ════════════════════════════════════════════════════════════════════════
#  1. 差分更新の取得日計画（batch_service._plan_fins_fetch_dates）
# ════════════════════════════════════════════════════════════════════════

def test_plan_bootstrap_backfills_gap_cluster():
    """状態ファイルなし（初回の修復）: 開示ゼロ営業日の密集区間を丸ごと取り直す。"""
    from services.batch_service import _plan_fins_fetch_dates

    holiday = _TS("2026-04-29")
    trading = [d for d in pd.bdate_range("2026-03-02", "2026-06-30") if d != holiday]
    # 4/15 まで毎営業日の開示あり（全件取得済み）、以降はボタンを押した日だけ
    disc = [d for d in trading if d <= _TS("2026-04-15")] + [_TS("2026-05-18"), _TS("2026-06-05")]
    # 3/12 だけは孤立した開示ゼロ日
    disc = [d for d in disc if d != _TS("2026-03-12")]

    plan = set(_plan_fins_fetch_dates(disc, trading, today="2026-06-30", verified_through=None))

    for d in trading:
        if d >= _TS("2026-04-16"):
            assert d in plan, f"欠落日 {d.date()} が取得対象にない"
    assert _TS("2026-05-18") in plan      # 当日分しか取れていない可能性のある日も取り直す
    assert _TS("2026-04-15") in plan      # 密集区間の直前マージン
    assert _TS("2026-03-12") in plan      # 孤立した欠落日はその日だけ取り直す
    assert holiday not in plan            # 取引日でない平日は取らない
    assert _TS("2026-03-10") not in plan  # 十分古い完備日は取らない


def test_plan_with_verified_through_fetches_overlap_and_new_days():
    """確認済み日がある場合: その前後の重なり + 以降の全営業日を取得する。"""
    from services.batch_service import _plan_fins_fetch_dates

    # prices は 9/14 まで（当日分の価格がまだ無くても当日の開示は取りに行く）
    trading = list(pd.bdate_range("2026-09-01", "2026-09-14"))
    plan = _plan_fins_fetch_dates(trading, trading, today="2026-09-15",
                                  verified_through="2026-09-09")
    assert plan == [_TS(d) for d in ["2026-09-08", "2026-09-09", "2026-09-10",
                                     "2026-09-11", "2026-09-14", "2026-09-15"]]


# ════════════════════════════════════════════════════════════════════════
#  2. update_fins（API をモックした結合テスト）
# ════════════════════════════════════════════════════════════════════════

def _setup_update_fins(tmp_path, monkeypatch, fake_get, verified_through="2026-09-01"):
    import services.batch_service as bs

    fins_path = tmp_path / "fins_cache.parquet"
    state_path = tmp_path / "fins_fetch_state.json"
    monkeypatch.setattr(bs, "FINS_PATH", str(fins_path))
    monkeypatch.setattr(bs, "FINS_STATE_PATH", str(state_path))
    pd.DataFrame([
        {"DiscNo": "1", "DiscDate": "2026-09-01", "Code": "10000", "DocType": _STMT, "Sales": "100"},
    ]).to_parquet(fins_path, index=False)
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump({"verified_through": verified_through}, f)
    monkeypatch.setattr(bs, "_get", fake_get)
    return bs, fins_path, state_path


def test_update_fins_backfills_days_when_button_not_pressed(tmp_path, monkeypatch):
    calls = []

    def fake_get(endpoint, params=None, retry=3):
        params = dict(params or {})
        calls.append(params)
        d = params["date"]
        if d == "2026-09-03" and "pagination_key" not in params:
            return {"data": [{"DiscNo": "3a", "DiscDate": d, "Code": "20000",
                              "DocType": _STMT, "Sales": "1"}],
                    "pagination_key": "next"}
        if d == "2026-09-03":
            return {"data": [{"DiscNo": "3b", "DiscDate": d, "Code": "30000",
                              "DocType": _STMT, "Sales": "2"}]}
        if d == "2026-09-01":
            # 重なり区間の再取得: 同じ DiscNo は新しい取得値で上書き
            return {"data": [{"DiscNo": "1", "DiscDate": d, "Code": "10000",
                              "DocType": _STMT, "Sales": "999"}]}
        return {"data": []}

    bs, fins_path, state_path = _setup_update_fins(tmp_path, monkeypatch, fake_get)
    trading = list(pd.bdate_range("2026-08-25", "2026-09-04"))

    out = bs.update_fins(trading_dates=trading, today="2026-09-04")

    fetched = {c["date"] for c in calls}
    # ボタンを押していない 9/2・9/3 も取得される（旧実装は当日 9/4 のみ）
    assert {"2026-09-02", "2026-09-03", "2026-09-04"} <= fetched
    # ページングを最後まで辿る
    assert {"3a", "3b"} <= set(out["DiscNo"])
    # DiscNo 重複は新しい値を残す
    one = out[out["DiscNo"] == "1"]
    assert len(one) == 1 and one.iloc[0]["Sales"] == "999"
    # 保存内容と戻り値が一致
    assert set(pd.read_parquet(fins_path)["DiscNo"]) == set(out["DiscNo"])
    # 当日（9/4）は後から開示が追加されうるので確認済みにしない
    with open(state_path, encoding="utf-8") as f:
        assert json.load(f)["verified_through"] == "2026-09-03"


def test_update_fins_watermark_stops_at_failed_day(tmp_path, monkeypatch):
    def fake_get(endpoint, params=None, retry=3):
        if params["date"] == "2026-09-02":
            raise RuntimeError("network error")
        return {"data": []}

    bs, _, state_path = _setup_update_fins(tmp_path, monkeypatch, fake_get)
    bs.update_fins(trading_dates=list(pd.bdate_range("2026-08-25", "2026-09-04")),
                   today="2026-09-04")
    with open(state_path, encoding="utf-8") as f:
        # 9/2 が失敗 → 9/3 が成功していても確認済みは 9/1 で止まり、次回 9/2 から取り直す
        assert json.load(f)["verified_through"] == "2026-09-01"


# ════════════════════════════════════════════════════════════════════════
#  3. 成長率の比較年度（batch_service._compute_metrics）
# ════════════════════════════════════════════════════════════════════════

def test_compute_metrics_forecast_growth_uses_immediately_prior_fy():
    """7803 型: 今期予想（予想修正レコード）は直前の確定FYと比較する。
    旧実装は FY2024/6 と比べて 売上 +22.1% / 利益 +586.1% になっていた。"""
    from services.batch_service import _compute_metrics

    code = "78030"
    fins = _df([
        _row(code, "2026-05-27", _EARN_REV, "2025-07-01", "2026-06-30",
             FSales=56_500.0, FNP=5_516.0, FEPS=40.65),
        _row(code, "2025-08-14", _STMT, "2024-07-01", "2025-06-30",
             NxtFYSt="2025-07-01", NxtFYEn="2026-06-30",
             Sales=56_175.0, OP=4_868.0, NP=3_418.0, EPS=49.7,
             NxFSales=56_000.0, NxFNp=2_700.0, **_BS),
        _row(code, "2024-08-13", _STMT, "2023-07-01", "2024-06-30",
             Sales=46_262.0, OP=2_000.0, NP=804.0, EPS=11.0, **_BS),
    ])
    row = _compute_metrics(code, _prices(code), fins, None)
    assert row is not None
    assert row["rev_growth"] == pytest.approx(0.6, abs=0.05)      # 56,500 / 56,175
    assert row["profit_growth"] == pytest.approx(61.4, abs=0.05)  # 5,516 / 3,418


def test_compute_metrics_next_fy_forecast_growth_uses_latest_fy():
    """135A 型: 本決算直後（来期予想 NxF*）は最新確定FYと比較する。
    旧実装は FY2025/2 と比べて 売上 +125.0% になっていた。"""
    from services.batch_service import _compute_metrics

    code = "135A0"
    fins = _df([
        _row(code, "2026-04-14", _STMT, "2025-03-01", "2026-02-28",
             NxtFYSt="2026-03-01", NxtFYEn="2027-02-28",
             Sales=3_278.0, OP=914.0, NP=652.0, EPS=63.95,
             NxFSales=4_823.0, NxFNp=972.0, **_BS),
        _row(code, "2025-04-14", _STMT, "2024-03-01", "2025-02-28",
             Sales=2_144.0, OP=594.0, NP=425.0, EPS=42.04, **_BS),
    ])
    row = _compute_metrics(code, _prices(code), fins, None)
    assert row["rev_growth"] == pytest.approx(47.1, abs=0.05)
    assert row["profit_growth"] == pytest.approx(49.1, abs=0.05)


def test_compute_metrics_growth_normalizes_irregular_fiscal_period():
    """325A 型: 決算期変更で前期が 7ヶ月（212日）→ 日数比で 12ヶ月ベースに揃える。
    旧実装は 12ヶ月の FY2025/1 と比べて 売上 +157.7% になっていた。"""
    from services.batch_service import _compute_metrics

    code = "325A0"
    fins = _df([
        _row(code, "2026-04-14", _Q2.replace("Consolidated", "NonConsolidated"),
             "2025-09-01", "2026-08-31", per_type="2Q",
             Sales=16_471.0, OP=2_189.0, NP=1_531.0, EPS=203.02,
             FSales=33_081.0, FNP=2_551.0, FEPS=333.2, **_BS),
        _row(code, "2025-10-15", _STMT, "2025-02-01", "2025-08-31",
             NxtFYSt="2025-09-01", NxtFYEn="2026-08-31",
             Sales=11_134.0, OP=1_167.0, NP=817.0, EPS=111.18, **_BS),
        _row(code, "2025-03-17", _STMT, "2024-02-01", "2025-01-31",
             Sales=12_837.0, OP=1_452.0, NP=1_061.0, EPS=160.11, **_BS),
    ])
    row = _compute_metrics(code, _prices(code), fins, None)
    exp_rev = (33_081.0 * 212 / 365 - 11_134.0) / 11_134.0 * 100   # ≈ +72.6%
    exp_np = (2_551.0 * 212 / 365 - 817.0) / 817.0 * 100            # ≈ +81.4%
    assert row["rev_growth"] == pytest.approx(exp_rev, abs=0.05)
    assert row["profit_growth"] == pytest.approx(exp_np, abs=0.05)


def test_period_length_scale():
    from services.fins_utils import period_length_scale

    # 通常の 12ヶ月同士（うるう年差）→ 補正なし
    assert period_length_scale("2023-04-01", "2024-03-31", "2022-04-01", "2023-03-31") == 1.0
    # REIT の 6ヶ月決算同士（181日 vs 184日）→ 補正なし
    assert period_length_scale("2026-02-01", "2026-07-31", "2025-08-01", "2026-01-31") == 1.0
    # 12ヶ月 vs 7ヶ月 → 前期の日数 / 今期の日数
    assert period_length_scale("2025-09-01", "2026-08-31",
                               "2025-02-01", "2025-08-31") == pytest.approx(212 / 365)
    # 13ヶ月決算 → 補正する
    assert period_length_scale("2025-03-01", "2026-03-31",
                               "2024-03-01", "2025-02-28") == pytest.approx(365 / 396)
    # 日付欠損 → 補正なし（従来挙動）
    assert period_length_scale(None, "2026-03-31", "2024-04-01", "2025-03-31") == 1.0


def test_build_fins_metrics_normalizes_irregular_period():
    """_build_fins_metrics の EPS 成長・増益判定も期間長を揃えて比較する。"""
    from services.pipeline_service import _build_fins_metrics

    code = "325A0"
    fins = _df([
        _row(code, "2026-10-15", _STMT, "2025-09-01", "2026-08-31",
             Sales=30_000.0, OP=150.0, EPS=200.0, EqAR=0.5, ShOutFY=1_000_000.0),
        _row(code, "2025-10-15", _STMT, "2025-02-01", "2025-08-31",
             Sales=11_134.0, OP=100.0, EPS=100.0, EqAR=0.5, ShOutFY=1_000_000.0),
    ])
    m = _build_fins_metrics(fins, _prices(code, end="2026-10-30"))
    row = m[m["code"] == code].iloc[0]
    assert row["eps_growth"] == pytest.approx((200.0 * 212 / 365 - 100.0) / 100.0 * 100)
    # 営業益 150（12ヶ月）は 7ヶ月換算で 87 < 100 → 増益ではない
    assert row["op_trend"] == 0
    assert bool(row["fy_irregular"]) is True


# ════════════════════════════════════════════════════════════════════════
#  4. 開示の鮮度判定（fins_utils）
# ════════════════════════════════════════════════════════════════════════

def test_disclosure_freshness_flags_overdue_statements():
    from services.fins_utils import disclosure_freshness

    rows = [
        # 5254 型: 最後の決算短信は 2Q（12/31 期末）。以降は予想修正だけ → 期限切れ
        {"Code": "52540", "DiscDate": "2026-02-12", "DocType": _Q2, "CurPerType": "2Q",
         "CurPerSt": "2025-07-01", "CurPerEn": "2025-12-31"},
        {"Code": "52540", "DiscDate": "2026-05-27", "DocType": _EARN_REV, "CurPerType": "FY",
         "CurPerSt": "2025-07-01", "CurPerEn": "2026-06-30"},
        # 通常: 1Q（6/30 期末）を 8/7 に開示 → 次の期限は 9/30 + 50日
        {"Code": "10000", "DiscDate": "2026-08-07", "DocType": _Q1, "CurPerType": "1Q",
         "CurPerSt": "2026-04-01", "CurPerEn": "2026-06-30"},
        # REIT: 6ヶ月決算（1/31 期末）→ 次の期限は 7/31 + 50日 = 9/19
        {"Code": "89510", "DiscDate": "2026-03-16", "DocType": _STMT, "CurPerType": "FY",
         "CurPerSt": "2025-08-01", "CurPerEn": "2026-01-31"},
    ]
    out = disclosure_freshness(pd.DataFrame(rows), as_of="2026-09-15").set_index("code")

    assert bool(out.loc["52540", "fins_overdue"]) is True
    assert out.loc["52540", "last_stmt_date"] == "2026-02-12"
    assert out.loc["52540", "last_disc_date"] == "2026-05-27"
    assert bool(out.loc["10000", "fins_overdue"]) is False
    assert bool(out.loc["89510", "fins_overdue"]) is False


def test_find_disclosure_gaps_uses_trading_days_only():
    from services.fins_utils import find_disclosure_gaps

    trading = [d for d in pd.bdate_range("2026-09-01", "2026-09-15") if d != _TS("2026-09-07")]
    disc = [d for d in trading if d not in (_TS("2026-09-02"), _TS("2026-09-03"))]
    gaps = find_disclosure_gaps(disc, trading, since="2026-09-01", until="2026-09-15")
    assert gaps == [_TS("2026-09-02"), _TS("2026-09-03")]
