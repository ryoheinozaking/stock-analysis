# -*- coding: utf-8 -*-
"""決算発表予定日による決算短信の未収録チェックと、予定日の差分取得のテスト（2026-09-19）"""
import pandas as pd

_Q1 = "1QFinancialStatements_Consolidated_JP"
_Q2 = "2QFinancialStatements_Consolidated_JP"
_FY = "FYFinancialStatements_Consolidated_JP"


def _stmt(code, disc, doctype, per_type, per_st, per_en):
    return {"Code": code, "DiscDate": disc, "DocType": doctype, "CurPerType": per_type,
            "CurPerSt": per_st, "CurPerEn": per_en}


def _sched(code, pub, sch, fq, fye):
    return {"Code": code, "PubDate": pub, "SchDate": sch, "FQName": fq, "FYE": fye}


def test_schedule_based_overdue_and_next_date():
    from services.fins_utils import disclosure_freshness

    fins = pd.DataFrame([
        # X: 1Q を予定どおり 8/7 に開示済み
        _stmt("X", "2026-08-07", _Q1, "1Q", "2026-04-01", "2026-06-30"),
        # Y（7803 型）: 本決算の予定日 8/13 を過ぎたのに、手元の最新は 2Q
        _stmt("Y", "2026-02-13", _Q2, "2Q", "2025-07-01", "2025-12-31"),
        # Z: 予定が 8/10 → 9/20 に延期。まだ予定日前なので未収録ではない
        _stmt("Z", "2026-05-14", _FY, "FY", "2025-04-01", "2026-03-31"),
        # W: 予定データが無い → 従来の推定ルール（2Q 期末 12/31 + 3ヶ月 + 50日 を過ぎて未収録）
        _stmt("W", "2026-02-12", _Q2, "2Q", "2025-07-01", "2025-12-31"),
    ])
    sched = pd.DataFrame([
        _sched("X", "2026-07-10", "2026-08-07", "1Q", "0331"),
        _sched("X", "2026-08-20", "2026-11-06", "2Q", "0331"),
        _sched("Y", "2026-07-15", "2026-08-13", "FY", "0630"),
        _sched("Z", "2026-07-01", "2026-08-10", "1Q", "0331"),
        _sched("Z", "2026-08-05", "2026-09-20", "1Q", "0331"),   # 延期の公表（新しい行として追加される）
    ])
    out = disclosure_freshness(fins, "2026-09-15", earnings_dates=sched).set_index("code")

    assert not out.loc["X", "fins_overdue"]
    assert out.loc["X", "next_sched_date"] == "2026-11-06"
    assert out.loc["Y", "fins_overdue"] and out.loc["Y", "missed_sched_date"] == "2026-08-13"
    assert not out.loc["Z", "fins_overdue"] and out.loc["Z", "next_sched_date"] == "2026-09-20"
    assert out.loc["W", "fins_overdue"] and pd.isna(out.loc["W", "missed_sched_date"])


def test_schedule_accepts_early_release_and_ignores_undecided_date():
    from services.fins_utils import disclosure_freshness

    fins = pd.DataFrame([
        # 予定日 8/14 より前倒しの 8/4 に 1Q を開示
        _stmt("A", "2026-08-04", _Q1, "1Q", "2026-04-01", "2026-06-30"),
        _stmt("B", "2026-05-14", _FY, "FY", "2025-04-01", "2026-03-31"),
        # R: 決算短信が1件も無い（財務情報に収録されないインフラファンド等）。予定修正の開示だけある
        _stmt("R", "2026-03-10", "EarnForecastRevision", "FY", "2025-08-01", "2026-07-31"),
    ])
    sched = pd.DataFrame([
        _sched("A", "2026-07-10", "2026-08-14", "1Q", "0331"),
        _sched("B", "2026-07-10", "", "1Q", "0331"),              # 未定（空欄）は判定に使わない
        _sched("R", "2026-07-10", "2026-08-17", "FY", "0731"),
    ])
    out = disclosure_freshness(fins, "2026-09-15", earnings_dates=sched).set_index("code")
    assert not out.loc["A", "fins_overdue"]
    assert not out.loc["B", "fins_overdue"] and pd.isna(out.loc["B", "next_sched_date"])
    assert not out.loc["R", "fins_overdue"] and pd.isna(out.loc["R", "missed_sched_date"])


def test_update_earnings_dates_resumes_from_last_pub_date_and_stops_at_failure(tmp_path, monkeypatch):
    import services.batch_service as bs

    path = tmp_path / "earnings_dates.parquet"
    monkeypatch.setattr(bs, "EARNINGS_DATES_PATH", str(path))
    pd.DataFrame([_sched("X", "2026-09-10", "2026-10-30", "2Q", "0331")]).assign(
        CoName="X社", CoNameEn="X").to_parquet(path, index=False)

    calls = []

    def fake_get(endpoint, params=None, retry=3):
        calls.append((endpoint, params["date"]))
        if params["date"] == "2026-09-14":
            raise RuntimeError("network error")
        return {"data": [dict(_sched("Y", params["date"], "2026-11-10", "2Q", "0331"),
                              CoName="Y社", CoNameEn="Y")]}

    monkeypatch.setattr(bs, "_get", fake_get)
    out = bs.update_earnings_dates(today="2026-09-15")

    assert all(ep == "/fins/earnings-date" for ep, _ in calls)
    # 最終公表日 9/10 から取り直し、9/14 の失敗で止める
    assert [d for _, d in calls] == ["2026-09-10", "2026-09-11", "2026-09-14"]
    assert set(out["PubDate"]) == {"2026-09-10", "2026-09-11"}
    assert len(pd.read_parquet(path)) == len(out) == 3
