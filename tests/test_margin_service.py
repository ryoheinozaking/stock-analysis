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
