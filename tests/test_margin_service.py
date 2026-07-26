# -*- coding: utf-8 -*-
"""JPX 信用 PDF パーサの単体テスト。"""
import os

import pandas as pd
import pytest

from services import margin_service


def test_extract_text_is_importable():
    from scripts.extract_pdf import extract_text
    assert callable(extract_text)
