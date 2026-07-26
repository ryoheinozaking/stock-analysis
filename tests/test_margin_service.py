# -*- coding: utf-8 -*-
"""JPX 信用 PDF パーサの単体テスト。"""
import os

import pandas as pd
import pytest

from services import margin_service


def test_extract_text_is_importable():
    from scripts.extract_pdf import extract_text
    assert callable(extract_text)


FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "margin_sample.txt")


def test_parse_margin_text_extracts_known_stock():
    with open(FIXTURE, encoding="utf-8") as f:
        text = f.read()
    df = margin_service.parse_margin_text(text)
    kyokuyo = df[df["code"] == "13010"].iloc[0]
    assert kyokuyo["sell_balance"] == 2800
    assert kyokuyo["buy_balance"] == 162700
    assert kyokuyo["sell_wow"] == -200
    assert kyokuyo["buy_wow"] == -700
    # 信用倍率 = 買残 / 売残
    assert kyokuyo["margin_ratio"] == pytest.approx(162700 / 2800, rel=1e-4)


def test_parse_margin_text_second_stock():
    with open(FIXTURE, encoding="utf-8") as f:
        text = f.read()
    df = margin_service.parse_margin_text(text)
    assert set(df["code"]) == {"13010", "13320"}
    nissui = df[df["code"] == "13320"].iloc[0]
    assert nissui["sell_balance"] == 76500
    assert nissui["buy_balance"] == 230900


def test_parse_margin_text_raises_on_garbage():
    with pytest.raises(ValueError):
        margin_service.parse_margin_text("これは信用PDFではないテキスト")


def test_load_latest_margin_picks_newest(tmp_path, monkeypatch):
    d = tmp_path / "margin"
    d.mkdir()
    old = pd.DataFrame([{"code": "13010", "margin_ratio": 1.0}])
    new = pd.DataFrame([{"code": "13010", "margin_ratio": 2.0}])
    old.to_parquet(d / "20260703.parquet")
    new.to_parquet(d / "20260717.parquet")
    monkeypatch.setattr(margin_service, "MARGIN_DIR", str(d))
    got = margin_service.load_latest_margin()
    assert got is not None
    assert got[got["code"] == "13010"].iloc[0]["margin_ratio"] == 2.0


def test_load_latest_margin_returns_none_when_empty(tmp_path, monkeypatch):
    d = tmp_path / "margin"
    d.mkdir()
    monkeypatch.setattr(margin_service, "MARGIN_DIR", str(d))
    assert margin_service.load_latest_margin() is None


def test_parse_margin_text_raises_on_negative_balance():
    # 列がずれて残高スロットに前週比(負値)を拾った状況を模擬 → 中断すべき(I2)。
    bad = "\n".join([
        "B", "テスト　普通株式", "99990", "JP9999999999",
        "▲ 500",   # 売残高スロットに負値(=列ずれ)
        "100", "200", "50",
    ])
    with __import__("pytest").raises(ValueError):
        margin_service.parse_margin_text(bad)


def test_parse_week_dates_extracts_and_sorts_desc():
    html = (
        'x <a href="/.../syumatsu2026061900.pdf">a</a> '
        '<a href="/.../syumatsu2026071700.pdf">b</a> '
        'dup <a href="/.../syumatsu2026070300.pdf">c</a> '
        '<a href="/.../syumatsu2026071700.pdf">dup</a>'
    )
    got = margin_service.parse_week_dates(html)
    assert got == ["20260717", "20260703", "20260619"]


def test_load_margin_history_concats_with_as_of(tmp_path, monkeypatch):
    d = tmp_path / "margin"
    d.mkdir()
    import pandas as pd
    pd.DataFrame([{"code": "13010", "buy_balance": 100, "sell_balance": 10}]).to_parquet(d / "20260703.parquet")
    pd.DataFrame([{"code": "13010", "buy_balance": 120, "sell_balance": 8}]).to_parquet(d / "20260717.parquet")
    monkeypatch.setattr(margin_service, "MARGIN_DIR", str(d))
    hist = margin_service.load_margin_history()
    assert hist is not None
    assert set(hist["as_of"]) == {"20260703", "20260717"}
    assert len(hist) == 2


def test_archived_weeks_desc(tmp_path, monkeypatch):
    d = tmp_path / "margin"
    d.mkdir()
    import pandas as pd
    for wk in ["20260703", "20260717", "20260619"]:
        pd.DataFrame([{"code": "13010"}]).to_parquet(d / f"{wk}.parquet")
    monkeypatch.setattr(margin_service, "MARGIN_DIR", str(d))
    assert margin_service.archived_weeks() == ["20260717", "20260703", "20260619"]
