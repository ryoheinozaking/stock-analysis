# -*- coding: utf-8 -*-
"""
バリュエーション指標（J-Quants /v2/equities/valuation）の取得と答え合わせのテスト
"""
import numpy as np
import pandas as pd


def _val_row(date, code, **vals):
    row = {"Date": date, "Code": code, "EPS": 10.0, "FwdEPS": 11.0, "BPS": 100.0,
           "ROE": 0.1, "FwdROE": 0.11, "PER": 10.0, "FwdPER": 9.0, "PBR": 1.0, "MktCap": 1000.0}
    row.update(vals)
    return row


def _setup(tmp_path, monkeypatch, fake_get, existing=None):
    import services.batch_service as bs

    path = tmp_path / "valuation.parquet"
    monkeypatch.setattr(bs, "VALUATION_PATH", str(path))
    monkeypatch.setattr(bs, "_get", fake_get)
    if existing is not None:
        existing.to_parquet(path, index=False)
    return bs, path


# ════════════════════════════════════════════════════════════════════════
#  batch_service.update_valuation
# ════════════════════════════════════════════════════════════════════════

def test_update_valuation_resumes_and_stops_at_failed_day(tmp_path, monkeypatch):
    calls = []

    def fake_get(endpoint, params=None, retry=3):
        params = dict(params or {})
        calls.append((endpoint, params))
        d = params["date"]
        if d == "2026-09-11" and "pagination_key" not in params:
            return {"data": [_val_row(d, "10000")], "pagination_key": "p2"}
        if d == "2026-09-11":
            return {"data": [_val_row(d, "20000")]}
        if d == "2026-09-14":
            raise RuntimeError("network error")
        return {"data": [_val_row(d, "10000")]}

    existing = pd.DataFrame([_val_row("2026-09-10", "10000")])
    bs, path = _setup(tmp_path, monkeypatch, fake_get, existing)

    out = bs.update_valuation(today="2026-09-15")

    assert all(ep == "/equities/valuation" for ep, _ in calls)
    # 最終日 9/10 の翌営業日から取得し（ページングも辿る）、9/14 の失敗で止める
    assert [p["date"] for _, p in calls] == ["2026-09-11", "2026-09-11", "2026-09-14"]
    assert set(zip(out["Date"], out["Code"])) == {
        ("2026-09-10", "10000"), ("2026-09-11", "10000"), ("2026-09-11", "20000"),
    }
    assert len(pd.read_parquet(path)) == 3

    # 次回は失敗した 9/14 から取り直す
    calls.clear()

    def fake_ok(endpoint, params=None, retry=3):
        calls.append((endpoint, dict(params)))
        return {"data": [_val_row(params["date"], "10000")]}

    monkeypatch.setattr(bs, "_get", fake_ok)
    out2 = bs.update_valuation(today="2026-09-15")
    assert [p["date"] for _, p in calls] == ["2026-09-14", "2026-09-15"]
    assert out2["Date"].max() == "2026-09-15"


def test_update_valuation_initial_window(tmp_path, monkeypatch):
    calls = []

    def fake_get(endpoint, params=None, retry=3):
        calls.append(params["date"])
        return {"data": []}   # 16:30 前の当日分・祝日は空で返る

    bs, path = _setup(tmp_path, monkeypatch, fake_get)
    out = bs.update_valuation(today="2026-09-15")

    assert out.empty and not path.exists()
    start = pd.Timestamp("2026-09-15") - pd.Timedelta(days=bs.VALUATION_INITIAL_DAYS)
    assert calls[0] == pd.bdate_range(start, "2026-09-15")[0].strftime("%Y-%m-%d")
    assert calls[-1] == "2026-09-15"


# ════════════════════════════════════════════════════════════════════════
#  valuation_audit: 答え合わせの分類
# ════════════════════════════════════════════════════════════════════════

def test_audit_valuation_classifies_known_differences():
    from services import valuation_audit as va

    codes = ["A", "B", "C", "D", "E", "F", "G"]
    ours = pd.DataFrame({
        "code": codes,
        "company_name": [f"社{c}" for c in codes],
        "PER":        [10.0, 10.0, 30.0, 20.0, 50.0, 50.0, 50.0],
        "PBR":        [1.0, 1.25, 1.0, 1.0, 1.0, 1.0, 1.0],
        "ROE":        [8.0] * 7,                       # 自前は%
        "market_cap": [1e10, 1.25e10] + [1e10] * 5,    # 自前は円
    })
    jpx = pd.DataFrame({
        "Date":   "2026-09-15",
        "Code":   codes,
        "FwdPER": [10.2, 10.0, 10.0, np.nan, 10.0, 10.0, 10.0],
        "PER":    [9.0] * 7,          # 実績PER。自前の列名と衝突しても壊れないこと
        "PBR":    [1.0] * 7,
        "FwdROE": [0.08] * 7,         # JPX は小数
        "MktCap": [1e4] * 7,          # JPX は百万円
    })
    refs = pd.DataFrame({
        "code":             codes,
        "treasury_ratio":   [1.0, 1.25] + [np.nan] * 5,
        "mcap_share_ratio": [1.0, 1.25] + [np.nan] * 5,
        "eq_ratio":         [1.0] * 7,
        "last_disc_date":   ["2026-08-01"] * 4 + ["2026-09-15", "2026-08-01", "2026-08-01"],
        "fins_overdue":     [False] * 5 + [True, False],
    })
    splits = pd.DataFrame({"code": codes, "split_factor": [1.0, 1.0, 1 / 3, 1.0, 1.0, 1.0, 1.0]})

    audit = va.audit_valuation(ours, jpx, refs, splits)
    cat = audit.set_index(["code", "metric"])["category"]

    assert cat[("A", "PER（予想）")] == va.CAT_MATCH
    assert cat[("A", "ROE（予想）")] == va.CAT_MATCH      # 8% と 0.08 の単位差を吸収
    assert cat[("A", "時価総額")] == va.CAT_MATCH         # 円と百万円の単位差を吸収
    assert cat[("B", "PBR")] == va.CAT_TREASURY
    assert cat[("B", "時価総額")] == va.CAT_TREASURY
    assert cat[("C", "PER（予想）")] == va.CAT_SPLIT        # 1:3 分割の調整漏れで 3 倍
    assert cat[("D", "PER（予想）")] == va.CAT_JPX_NULL
    assert cat[("E", "PER（予想）")] == va.CAT_JPX_LAG      # 評価日当日の開示は JPX 未反映
    assert cat[("F", "PER（予想）")] == va.CAT_OURS_STALE
    assert cat[("G", "PER（予想）")] == va.CAT_UNKNOWN

    summary = va.summarize(audit).set_index("metric")
    assert summary.loc["PER（予想）", "比較銘柄数"] == 7
    assert summary.loc["PER（予想）", "一致"] == 2
    assert summary.loc["PER（予想）", va.CAT_UNKNOWN] == 1


def test_build_reference_ratios_treasury_and_equity():
    from services.valuation_audit import build_reference_ratios

    fins = pd.DataFrame([
        {"Code": "72030", "DiscDate": "2026-05-08", "DocType": "FYFinancialStatements_Consolidated_IFRS",
         "CurPerType": "FY", "CurPerSt": "2025-04-01", "CurPerEn": "2026-03-31",
         "ShOutFY": 1_000.0, "TrShFY": 200.0, "Eq": 5_000.0},
        {"Code": "72030", "DiscDate": "2026-08-07", "DocType": "1QFinancialStatements_Consolidated_IFRS",
         "CurPerType": "1Q", "CurPerSt": "2026-04-01", "CurPerEn": "2026-06-30",
         "ShOutFY": 900.0, "TrShFY": 150.0, "Eq": 5_500.0},
    ])
    refs = build_reference_ratios(fins, as_of="2026-09-15").set_index("code")
    assert refs.loc["72030", "treasury_ratio"] == 900.0 / 750.0     # 直近の決算短信
    assert refs.loc["72030", "mcap_share_ratio"] == 1_000.0 / 750.0  # 本決算の株数 ÷ 直近の自己株控除後
    assert refs.loc["72030", "eq_ratio"] == 5_500.0 / 5_000.0
    assert refs.loc["72030", "last_disc_date"] == "2026-08-07"
