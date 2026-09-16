# -*- coding: utf-8 -*-
"""J-Quants 一括ダウンロード（services/bulk_service.py）のテスト"""
import json
import os

import pytest


class _FakeResp:
    def __init__(self, content: bytes):
        self._content = content

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield self._content


def _setup(monkeypatch, listing, payloads):
    """listing: /bulk/list の返り値、payloads: Key → ダウンロード内容（bytes）"""
    import services.bulk_service as bs

    calls = {"list": [], "get": []}

    def fake_get_all(endpoint, params=None):
        calls["list"].append(dict(params))
        return list(listing)

    def fake_get(endpoint, params=None, retry=3):
        assert endpoint == "/bulk/get"
        calls["get"].append(params["key"])
        return {"url": "https://signed.example/" + params["key"]}

    def fake_requests_get(url, stream=True, timeout=120):
        return _FakeResp(payloads[url.replace("https://signed.example/", "")])

    monkeypatch.setattr(bs, "_get_all", fake_get_all)
    monkeypatch.setattr(bs, "_get", fake_get)
    monkeypatch.setattr(bs.requests, "get", fake_requests_get)
    return bs, calls


_K1 = "equities/valuation/historical/2026/equities_valuation_202607.csv.gz"
_K2 = "equities/valuation/historical/2026/equities_valuation_202608.csv.gz"


def test_sync_downloads_then_skips_unchanged_and_refetches_updated(tmp_path, monkeypatch):
    listing = [
        {"Key": _K1, "LastModified": "2026-08-01T00:00:00+00:00", "Size": 3},
        {"Key": _K2, "LastModified": "2026-09-01T00:00:00+00:00", "Size": 4},
    ]
    payloads = {_K1: b"abc", _K2: b"defg"}
    bs, calls = _setup(monkeypatch, listing, payloads)

    res = bs.sync_bulk("/equities/valuation", "2026-07", "2026-08", root=str(tmp_path), sleep=lambda s: None)
    assert res["downloaded"] == [_K1, _K2] and not res["failed"]
    assert calls["list"] == [{"endpoint": "/equities/valuation", "from": "2026-07", "to": "2026-08"}]
    assert (tmp_path / _K1).read_bytes() == b"abc"
    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest[_K2] == {"LastModified": "2026-09-01T00:00:00+00:00", "Size": 4}

    # 2 回目: 変更なしは取り直さない
    calls["get"].clear()
    res = bs.sync_bulk("/equities/valuation", root=str(tmp_path), sleep=lambda s: None)
    assert res["skipped"] == [_K1, _K2] and calls["get"] == []

    # 訂正で 8 月分が上書きされた（LastModified が変わった）→ そのファイルだけ取り直す
    listing[1]["LastModified"] = "2026-09-10T00:00:00+00:00"
    listing[1]["Size"] = 5
    payloads[_K2] = b"defgh"
    res = bs.sync_bulk("/equities/valuation", root=str(tmp_path), sleep=lambda s: None)
    assert res["downloaded"] == [_K2] and res["skipped"] == [_K1]
    assert (tmp_path / _K2).read_bytes() == b"defgh"


def test_sync_rejects_unsafe_key_and_size_mismatch(tmp_path, monkeypatch):
    bad = "../../outside.csv.gz"
    short = "fins/summary/historical/2026/fins_summary_202608.csv.gz"
    listing = [
        {"Key": bad, "LastModified": "x", "Size": 1},
        {"Key": short, "LastModified": "y", "Size": 10},   # 一覧は 10 バイトなのに 3 バイトしか来ない
    ]
    bs, _ = _setup(monkeypatch, listing, {bad: b"z", short: b"abc"})

    res = bs.sync_bulk("/fins/summary", root=str(tmp_path), sleep=lambda s: None)

    failed = dict(res["failed"])
    assert "不正なファイルキー" in failed[bad]
    assert "サイズ不一致" in failed[short]
    assert not res["downloaded"]
    assert not (tmp_path / short).exists()                 # 壊れたファイルは残さない
    assert not os.path.exists(os.path.join(str(tmp_path), "..", "..", "outside.csv.gz"))
    assert not (tmp_path / "manifest.json").exists()       # 失敗分は記録しない（次回また取りに行く）
