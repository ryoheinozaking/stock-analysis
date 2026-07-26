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
