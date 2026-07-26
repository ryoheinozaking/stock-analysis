# セクター回転検知ダッシュボード Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** KabuTrend /trend を参考 UI とし、自前データ（prices.parquet + stock_cache.parquet + JPX 週次信用 PDF）で日本株のセクター回転・需給ランキングを可視化する Streamlit ページを作る。

**Architecture:** 計算ロジックを純粋関数 `services/rotation_service.py` に隔離（Streamlit 非依存・単体テスト可能）。ページ `pages/9_sector_rotation.py` は `@st.cache_data` ラッパー + Plotly 描画のみ。段階2 の信用データは JPX 週次 PDF を `scripts/extract_pdf.extract_text()` で抽出 → `services/margin_service.py` でパース → `data/margin/{YYYYMMDD}.parquet` に週次アーカイブ。margin アーカイブ不在でも段階1 は動作（グレースフルデグレード）。

**Tech Stack:** Python 3.9.1（`Optional[X]` 必須・`X | Y` 不可）, pandas, Streamlit, Plotly, PyMuPDF（pymupdf）, pytest。既存 `services/split_adjust.py`（分割正規化）と `services/batch_service.py`（parquet ロード）を再利用。

**設計書:** `docs/superpowers/specs/2026-07-26-sector-rotation-dashboard-design.md`

---

## File Structure

| ファイル | 責務 | 新規/変更 |
|---|---|---|
| `services/rotation_service.py` | セクター回転・ランキング・銘柄需給の純粋計算 | 新規 |
| `services/margin_service.py` | JPX 信用 PDF のDL・パース・アーカイブ・信用指標算出 | 新規 |
| `scripts/extract_pdf.py` | `extract_text()` 関数を公開（`fitz` import を集約） | 変更（軽微リファクタ） |
| `pages/9_sector_rotation.py` | Streamlit UI（キャッシュ + Plotly + ドリルダウン） | 新規 |
| `tests/test_rotation_service.py` | rotation_service 単体テスト | 新規 |
| `tests/test_margin_service.py` | margin_service パーサ単体テスト | 新規 |
| `tests/fixtures/margin_sample.txt` | JPX PDF 抽出テキストの軽量サンプル（数銘柄） | 新規 |
| `requirements.txt` | `pymupdf` 追加 | 変更 |
| `.gitignore` | `data/margin/` 追加 | 変更 |

### MVP スコープ注記（spec との対応・silently drop しない）

spec の全項目のうち、本計画で**この初回実装（MVP）に含めるもの / 高速フォローに回すもの**を明示する:

- **含む**: 回転4指標（出来高急増・鮮度・資金流入・テーマ温度）、テーマ内順位 self_rank、
  銘柄需給の主要3種（MA25乖離・52週高値/安値距離・新高値）、ランキング2種
  （モメンタム・出来高急増）、ドリルダウン `switch_page` 遷移、段階2 信用パイプライン一式。
- **高速フォロー（同じヘルパーで拡張・別タスク化）**: ランキングの残り3タブ
  （値上がり/値下がり/資金フロー）は per-stock 日次騰落が要るため prices からの
  当日リターン算出を足して `compute_rankings` に追加する。GC/DC シグナル
  （`stock_cache.mom_gc`）は `compute_stock_signals` に列追加するだけで足せる。
  MVP では self_rank 機構と2ランキングで「テーマ内順位」の核を先に通す。

### 主要データ形状（実データで確認済み）

- `prices.parquet`: `Date`(str 'YYYY-MM-DD'), `Code`(str 5桁 '13010'), `O/H/L/C`, `Vo`, `Va`(売買代金), `AdjFactor`。約504万行・5036銘柄。
- `stock_cache.parquet`: `code`(5桁・prices.Code と一致), `code_4`, `sector`(S33日本語名), `company_name`, `market`, `close`, `RSI`, `MA25`, `mom_*`, `sepa_from_low`(52週安値からの%), `sepa_from_high`(52週高値からの%・負値), `mom_new_high`(bool)。3751銘柄。
- `services.split_adjust.normalize_close(cp, dropna=True)`: 1銘柄分の価格 DataFrame（`C`, `AdjFactor` 必須）を末尾日スケールの close 系列に正規化。
- `services.batch_service.PRICES_PATH` / `load_cache()`（stock_cache を返す）/ `get_cache_updated_at()`。

---

## Phase 0: 環境準備

### Task 1: 依存・gitignore・fixtures ディレクトリ整備

**Files:**
- Modify: `requirements.txt`
- Modify: `.gitignore`
- Create: `tests/fixtures/.gitkeep`

- [ ] **Step 1: requirements.txt に pymupdf を追加**

`requirements.txt` の `pypdf>=3.0.0` の行の直後に追記:

```
pymupdf>=1.24.0
```

- [ ] **Step 2: pymupdf をローカル venv にインストール**

Run:
```bash
.venv/Scripts/python.exe -m pip install --trusted-host pypi.org --trusted-host files.pythonhosted.org "pymupdf>=1.24.0"
```
Expected: `Successfully installed pymupdf-...`

- [ ] **Step 3: インポート確認**

Run:
```bash
.venv/Scripts/python.exe -c "import fitz; print(fitz.__doc__.splitlines()[0])"
```
Expected: PyMuPDF のバージョン行が表示され、エラーなし。

- [ ] **Step 4: .gitignore に data/margin/ を追加**

`.gitignore` の `data/diagnose_growth/` の行の直後に追記:

```
data/margin/
```

- [ ] **Step 5: fixtures ディレクトリを作成**

Run:
```bash
mkdir -p tests/fixtures && touch tests/fixtures/.gitkeep
```

- [ ] **Step 6: Commit**

```bash
git add requirements.txt .gitignore tests/fixtures/.gitkeep
git commit -m "chore(deps): pymupdf追加・data/margin gitignore・fixtures dir"
```

---

## Phase 1: rotation_service（段階1・純粋計算）

### Task 2: セクター日次系列の集計 `load_sector_daily`

各営業日・各業種の等加重リターン・売買代金合計・構成銘柄数・上昇銘柄比率(breadth)を集計する。分割スケール破綻を避けるため `normalize_close` を各銘柄に適用してから pct_change を取る。

**Files:**
- Create: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

`tests/test_rotation_service.py`:

```python
# -*- coding: utf-8 -*-
"""services.rotation_service の単体テスト（合成データ）。"""
import numpy as np
import pandas as pd
import pytest

from services import rotation_service as rs


def _mk_prices(rows):
    """rows: list of (date, code, close, va, adjfactor) → prices風 DataFrame。"""
    return pd.DataFrame(
        rows, columns=["Date", "Code", "C", "Va", "AdjFactor"]
    )


def test_load_sector_daily_equal_weight_return():
    # 2業種・各2銘柄・3営業日。分割なし(AdjFactor=1)。
    # 銘柄A1: 100→110(+10%)→121(+10%), A2: 100→90(-10%)→99(+10%)
    # 業種X の day2 等加重リターン = (+10% + -10%)/2 = 0
    rows = [
        ("2026-01-05", "0001", 100.0, 1000.0, 1.0),
        ("2026-01-06", "0001", 110.0, 1200.0, 1.0),
        ("2026-01-07", "0001", 121.0, 1300.0, 1.0),
        ("2026-01-05", "0002", 100.0, 500.0, 1.0),
        ("2026-01-06", "0002", 90.0, 600.0, 1.0),
        ("2026-01-07", "0002", 99.0, 700.0, 1.0),
    ]
    prices = _mk_prices(rows)
    sector_map = {"0001": "X", "0002": "X"}
    out = rs.load_sector_daily(prices, sector_map, window_days=10)
    day2 = out[(out["Date"] == "2026-01-06") & (out["sector"] == "X")].iloc[0]
    assert day2["ret"] == pytest.approx(0.0, abs=1e-9)
    assert day2["va"] == pytest.approx(1800.0)  # 1200 + 600
    assert day2["n"] == 2
    assert day2["up_ratio"] == pytest.approx(0.5)  # 1銘柄上昇/2


def test_load_sector_daily_split_scale_safe():
    # 銘柄が day3 で 1:2 分割。AdjFactor は分割日に 0.5 が入る想定。
    # 生Cは 200→220→(分割後)120 だが正規化リターンは +10%,+9.09% になるべき。
    rows = [
        ("2026-01-05", "0001", 200.0, 1000.0, 1.0),
        ("2026-01-06", "0001", 220.0, 1000.0, 1.0),
        ("2026-01-07", "0001", 120.0, 1000.0, 0.5),
    ]
    prices = _mk_prices(rows)
    sector_map = {"0001": "X"}
    out = rs.load_sector_daily(prices, sector_map, window_days=10)
    day3 = out[(out["Date"] == "2026-01-07") & (out["sector"] == "X")].iloc[0]
    # 分割調整後の実質: 220相当→240相当なので +9.09%。生の 220→120 の -45% ではない。
    assert day3["ret"] > 0.05
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py -v`
Expected: FAIL（`ModuleNotFoundError` または `AttributeError: module 'services.rotation_service' has no attribute 'load_sector_daily'`）

- [ ] **Step 3: Write minimal implementation**

`services/rotation_service.py`:

```python
# -*- coding: utf-8 -*-
"""セクター回転・需給ランキングの純粋計算（Streamlit 非依存）。

KabuTrend /trend を参考 UI としつつ、指標定義・閾値は自前データで決める。
分割スケール破綻を避けるため、リターンは split_adjust.normalize_close 経由で算出する。
"""
from __future__ import annotations

from typing import Dict, Optional

import numpy as np
import pandas as pd

from services import split_adjust

# ── パラメータ（後でバックテストで調整可能なよう外出し） ──
DEFAULT_WINDOW_DAYS = 90
MIN_STOCKS_PER_SECTOR = 3
WEEK_DAYS = 5
MONTH_DAYS = 20
TURNOVER_MEDIAN_DAYS = 20


def load_sector_daily(
    prices: pd.DataFrame,
    sector_map: Dict[str, str],
    window_days: int = DEFAULT_WINDOW_DAYS,
) -> pd.DataFrame:
    """各 (Date, sector) の等加重リターン・売買代金合計・銘柄数・上昇比率を返す。

    Returns 列: Date, sector, ret, va, n, up_ratio
    """
    df = prices[["Date", "Code", "C", "Va", "AdjFactor"]].copy()
    # 直近 window_days 営業日に絞る
    dates = sorted(df["Date"].unique())[-window_days:]
    df = df[df["Date"].isin(dates)]
    df = df.sort_values(["Code", "Date"])

    # 銘柄ごとに分割正規化 close → pct_change
    def _ret(cp: pd.DataFrame) -> pd.Series:
        norm = split_adjust.normalize_close(cp, dropna=False)
        return norm.pct_change()

    df["ret"] = df.groupby("Code", group_keys=False).apply(_ret)
    df["sector"] = df["Code"].map(sector_map)
    df = df.dropna(subset=["sector"])

    grp = df.groupby(["Date", "sector"])
    out = grp.agg(
        ret=("ret", "mean"),
        va=("Va", "sum"),
        n=("Code", "size"),
        up_ratio=("ret", lambda s: float((s > 0).mean())),
    ).reset_index()
    # 構成銘柄数が閾値未満の業種は除外
    out = out[out["n"] >= MIN_STOCKS_PER_SECTOR].reset_index(drop=True)
    return out
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py -v`
Expected: `test_load_sector_daily_equal_weight_return` PASS。`test_load_sector_daily_split_scale_safe` PASS。

> 注: `MIN_STOCKS_PER_SECTOR=3` だとテストの2銘柄業種が除外される。テストは `window_days` を渡すが銘柄数閾値には掛からないよう、**Step 3 の `load_sector_daily` に引数 `min_stocks: int = MIN_STOCKS_PER_SECTOR` を追加し、テストからは `min_stocks=1` を渡す**。Step 1 のテスト2箇所の呼び出しを `rs.load_sector_daily(prices, sector_map, window_days=10, min_stocks=1)` に修正し、Step 3 のシグネチャと `out = out[out["n"] >= min_stocks]` に反映すること。

- [ ] **Step 5: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): セクター日次系列 load_sector_daily(分割正規化・等加重)"
```

---

### Task 3: 出来高急増 `compute_volume_surge`

当日セクター売買代金 ÷ 直近中央値 = turnoverRatio。降順ランク。

**Files:**
- Modify: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

`tests/test_rotation_service.py` に追記:

```python
def test_compute_volume_surge_ratio():
    # 業種X: 過去5日 va=100 一定 → 中央値100。最終日 va=270 → ratio 2.7。
    rows = []
    for i, d in enumerate(["01-01", "01-02", "01-03", "01-04", "01-05"]):
        va = 100.0 if d != "01-05" else 270.0
        rows.append((f"2026-{d}", "X", 0.0, va, 5, 0.5))
    sd = pd.DataFrame(rows, columns=["Date", "sector", "ret", "va", "n", "up_ratio"])
    out = rs.compute_volume_surge(sd, median_days=4)
    row = out[out["sector"] == "X"].iloc[0]
    assert row["turnover_ratio"] == pytest.approx(2.7)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_volume_surge_ratio -v`
Expected: FAIL（`has no attribute 'compute_volume_surge'`）

- [ ] **Step 3: Write minimal implementation**

`services/rotation_service.py` に追記:

```python
def compute_volume_surge(
    sector_daily: pd.DataFrame,
    median_days: int = TURNOVER_MEDIAN_DAYS,
) -> pd.DataFrame:
    """各業種の turnover_ratio = 最終日売買代金 ÷ 直近 median_days 日の中央値。降順。

    Returns 列: sector, turnover_ratio, va, daily_return_pct
    """
    rows = []
    for sector, g in sector_daily.sort_values("Date").groupby("sector"):
        va = g["va"].to_numpy(dtype="float64")
        if len(va) < 2:
            continue
        hist = va[-(median_days + 1):-1] if len(va) > median_days else va[:-1]
        med = float(np.median(hist)) if len(hist) else np.nan
        ratio = float(va[-1] / med) if med and med > 0 else np.nan
        rows.append({
            "sector": sector,
            "turnover_ratio": ratio,
            "va": float(va[-1]),
            "daily_return_pct": float(g["ret"].iloc[-1] * 100.0),
        })
    out = pd.DataFrame(rows).sort_values("turnover_ratio", ascending=False)
    return out.reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_volume_surge_ratio -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): 出来高急増 compute_volume_surge(中央値比)"
```

---

### Task 4: 鮮度 `compute_freshness`

週リターン(5日累積)・月リターン(20日累積)でランクを取り、`rankDelta = weekRank − monthRank`（負=最近急浮上）。上昇中/勝ち続け/失速に3分類。

**Files:**
- Modify: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

```python
def _mk_sector_series(sector, rets):
    """rets: 日次リターン list → sector_daily 風の行群。"""
    rows = []
    for i, r in enumerate(rets):
        rows.append((f"2026-02-{i+1:02d}", sector, r, 100.0, 5, 0.5))
    return rows


def test_compute_freshness_rising_vs_fading():
    # A: 直近1週だけ強い(月では平凡) → 上昇中。B: 月は強いが直近失速 → 失速。
    rets_A = [0.0] * 15 + [0.03] * 5           # 月20日中、後半5日だけ+3%
    rets_B = [0.03] * 15 + [-0.03] * 5          # 前半強く直近マイナス
    rows = _mk_sector_series("A", rets_A) + _mk_sector_series("B", rets_B)
    sd = pd.DataFrame(rows, columns=["Date", "sector", "ret", "va", "n", "up_ratio"])
    out = rs.compute_freshness(sd, week_days=5, month_days=20)
    a = out[out["sector"] == "A"].iloc[0]
    b = out[out["sector"] == "B"].iloc[0]
    # A は週ランクが月ランクより上位(=数値小) → rank_delta 負
    assert a["rank_delta"] < 0
    assert a["category"] == "rising"
    # B は週ランクが月ランクより下位 → rank_delta 正 → 失速
    assert b["rank_delta"] > 0
    assert b["category"] == "falling"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_freshness_rising_vs_fading -v`
Expected: FAIL（`has no attribute 'compute_freshness'`）

- [ ] **Step 3: Write minimal implementation**

```python
def _cum_return(ret_series: pd.Series, days: int) -> float:
    tail = ret_series.tail(days)
    return float((1.0 + tail).prod() - 1.0)


def compute_freshness(
    sector_daily: pd.DataFrame,
    week_days: int = WEEK_DAYS,
    month_days: int = MONTH_DAYS,
) -> pd.DataFrame:
    """週/月の累積リターンからランクを取り、rank_delta と3分類を返す。

    category: rising(週良・月悪) / winning(両方良) / falling(週悪・月良) / neutral
    Returns 列: sector, week_return, month_return, week_rank, month_rank,
                rank_delta, category
    """
    rows = []
    for sector, g in sector_daily.sort_values("Date").groupby("sector"):
        rows.append({
            "sector": sector,
            "week_return": _cum_return(g["ret"], week_days),
            "month_return": _cum_return(g["ret"], month_days),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    # ランク（1 = 最良）。降順リターン → method='min'
    out["week_rank"] = out["week_return"].rank(ascending=False, method="min")
    out["month_rank"] = out["month_return"].rank(ascending=False, method="min")
    out["rank_delta"] = out["week_rank"] - out["month_rank"]

    n = len(out)
    top = max(1, int(round(n * 0.25)))  # 上位25%を「良」とする自前分位点

    def _classify(r):
        week_good = r["week_rank"] <= top
        month_good = r["month_rank"] <= top
        if week_good and month_good:
            return "winning"
        if week_good and not month_good:
            return "rising"
        if not week_good and month_good:
            return "falling"
        return "neutral"

    out["category"] = out.apply(_classify, axis=1)
    return out.sort_values("week_rank").reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_freshness_rising_vs_fading -v`
Expected: PASS

> 注: このテストは2業種のみ（n=2, top=1）。A は週リターン高→week_rank=1, month_rank=2 → rank_delta=-1, category="rising"。B は逆で rank_delta=+1, category="falling"。境界が合わない場合は `top = max(1, ...)` の分位点を確認。

- [ ] **Step 5: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): 鮮度 compute_freshness(週月順位差・3分類)"
```

---

### Task 5: 資金流入スコア `compute_fund_flow`

5成分（売買代金ペース・持続日数・直近リターン・上昇比率(breadth)・上昇売買代金シェア）を業種内相対値で 0-100 に正規化し合成。**flowDominance という語は使わず `up_turnover_share` とする**。

**Files:**
- Modify: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

```python
def test_compute_fund_flow_score_bounds_and_components():
    # 3業種、最終日に売買代金が急増しbreadthも高い業種が高スコアになること。
    rows = []
    for s, pace_last, up in [("A", 300.0, 0.9), ("B", 100.0, 0.5), ("C", 50.0, 0.1)]:
        for i in range(20):
            va = 100.0 if i < 19 else pace_last
            ret = 0.02 if (i == 19 and up > 0.5) else 0.0
            rows.append((f"2026-03-{i+1:02d}", s, ret, va, 10, up))
    sd = pd.DataFrame(rows, columns=["Date", "sector", "ret", "va", "n", "up_ratio"])
    # up_turnover_share は sector_daily に無いので、テストでは up_ratio で近似する引数を使う
    out = rs.compute_fund_flow(sd)
    assert out["score"].between(0, 100).all()
    # A が最高スコア
    assert out.sort_values("score", ascending=False).iloc[0]["sector"] == "A"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_fund_flow_score_bounds_and_components -v`
Expected: FAIL（`has no attribute 'compute_fund_flow'`）

- [ ] **Step 3: Write minimal implementation**

```python
def _minmax_0_100(s: pd.Series) -> pd.Series:
    lo, hi = s.min(), s.max()
    if hi <= lo:
        return pd.Series(50.0, index=s.index)
    return (s - lo) / (hi - lo) * 100.0


def compute_fund_flow(
    sector_daily: pd.DataFrame,
    median_days: int = TURNOVER_MEDIAN_DAYS,
) -> pd.DataFrame:
    """資金流入5成分を業種内相対で正規化し合成スコア(0-100)を返す。

    成分: turnover_pace / persistence / flow_return / breadth / up_turnover_share
    ※ up_turnover_share は sector_daily に上昇売買代金列が無い場合 up_ratio で代替。
    Returns 列: sector, score, turnover_pace, persistence, flow_return,
                breadth, up_turnover_share
    """
    rows = []
    for sector, g in sector_daily.sort_values("Date").groupby("sector"):
        va = g["va"].to_numpy(dtype="float64")
        hist = va[-(median_days + 1):-1] if len(va) > median_days else va[:-1]
        avg = float(np.mean(hist)) if len(hist) else np.nan
        pace = float(va[-1] / avg) if avg and avg > 0 else 1.0
        # 持続日数: 直近から連続で平均を上回った日数
        persistence = 0
        if avg and avg > 0:
            for v in va[::-1]:
                if v > avg:
                    persistence += 1
                else:
                    break
        rows.append({
            "sector": sector,
            "turnover_pace": pace,
            "persistence": float(persistence),
            "flow_return": float(g["ret"].iloc[-1]),
            "breadth": float(g["up_ratio"].iloc[-1]),
            "up_turnover_share": float(g["up_ratio"].iloc[-1]),
        })
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    comp = pd.DataFrame({
        "c_pace": _minmax_0_100(out["turnover_pace"]),
        "c_persist": _minmax_0_100(out["persistence"]),
        "c_return": _minmax_0_100(out["flow_return"]),
        "c_breadth": _minmax_0_100(out["breadth"]),
        "c_share": _minmax_0_100(out["up_turnover_share"]),
    })
    out["score"] = comp.mean(axis=1)
    return out.sort_values("score", ascending=False).reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_fund_flow_score_bounds_and_components -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): 資金流入 compute_fund_flow(5成分・業種内相対)"
```

---

### Task 6: テーマ温度 `compute_theme_temperature`

上昇業種比率と中央 breadth を 0-100 に合成。

**Files:**
- Modify: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

```python
def test_theme_temperature_range_and_direction():
    # 全業種プラス&高breadth → 高温。全業種マイナス&低breadth → 低温。
    hot_rows, cold_rows = [], []
    for s in ["A", "B", "C", "D"]:
        hot_rows.append((s, 0.02, 0.9))
        cold_rows.append((s, -0.02, 0.1))
    hot = pd.DataFrame(hot_rows, columns=["sector", "period_return", "breadth"])
    cold = pd.DataFrame(cold_rows, columns=["sector", "period_return", "breadth"])
    t_hot = rs.compute_theme_temperature(hot)
    t_cold = rs.compute_theme_temperature(cold)
    assert 0 <= t_cold < t_hot <= 100
    assert t_hot > 60
    assert t_cold < 40
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_theme_temperature_range_and_direction -v`
Expected: FAIL（`has no attribute 'compute_theme_temperature'`）

- [ ] **Step 3: Write minimal implementation**

```python
def compute_theme_temperature(sector_summary: pd.DataFrame) -> float:
    """市況の体温計(0-100)。上昇業種比率と中央breadthの平均。

    sector_summary 列: sector, period_return, breadth
    """
    if sector_summary.empty:
        return 50.0
    advancing = float((sector_summary["period_return"] > 0).mean()) * 100.0
    med_breadth = float(sector_summary["breadth"].median()) * 100.0
    return round((advancing + med_breadth) / 2.0, 1)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_theme_temperature_range_and_direction -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): テーマ温度 compute_theme_temperature"
```

---

### Task 7: 銘柄需給シグナル `compute_stock_signals`

`stock_cache` から銘柄レベルの需給シグナル（52週高値/安値距離・新高値・MA25乖離）を組み立てる。52週は既存 `sepa_from_low/high`・`mom_new_high` を再利用（260日再計算しない）。

**Files:**
- Modify: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

```python
def test_compute_stock_signals_uses_sepa_columns():
    sc = pd.DataFrame([
        {"code": "0001", "code_4": "0001", "company_name": "テストA",
         "sector": "X", "close": 110.0, "MA25": 100.0, "RSI": 55.0,
         "sepa_from_low": 40.0, "sepa_from_high": -2.0, "mom_new_high": True},
        {"code": "0002", "code_4": "0002", "company_name": "テストB",
         "sector": "X", "close": 90.0, "MA25": 100.0, "RSI": 45.0,
         "sepa_from_low": 3.0, "sepa_from_high": -30.0, "mom_new_high": False},
    ])
    out = rs.compute_stock_signals(sc)
    a = out[out["code"] == "0001"].iloc[0]
    # MA25乖離率 = (110-100)/100*100 = +10%
    assert a["ma25_dev_pct"] == pytest.approx(10.0)
    assert bool(a["new_high"]) is True
    assert a["from_52w_high_pct"] == pytest.approx(-2.0)
    assert a["from_52w_low_pct"] == pytest.approx(40.0)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_stock_signals_uses_sepa_columns -v`
Expected: FAIL（`has no attribute 'compute_stock_signals'`）

- [ ] **Step 3: Write minimal implementation**

```python
def compute_stock_signals(stock_cache: pd.DataFrame) -> pd.DataFrame:
    """stock_cache から銘柄レベル需給シグナルを組み立てる。

    Returns 列: code, code_4, company_name, sector, close, RSI,
                ma25_dev_pct, from_52w_high_pct, from_52w_low_pct, new_high
    """
    df = stock_cache.copy()
    close = pd.to_numeric(df.get("close"), errors="coerce")
    ma25 = pd.to_numeric(df.get("MA25"), errors="coerce")
    df["ma25_dev_pct"] = (close - ma25) / ma25 * 100.0
    df["from_52w_high_pct"] = pd.to_numeric(df.get("sepa_from_high"), errors="coerce")
    df["from_52w_low_pct"] = pd.to_numeric(df.get("sepa_from_low"), errors="coerce")
    df["new_high"] = df.get("mom_new_high", False).astype("boolean").fillna(False)
    cols = ["code", "code_4", "company_name", "sector", "close", "RSI",
            "ma25_dev_pct", "from_52w_high_pct", "from_52w_low_pct", "new_high"]
    return df[[c for c in cols if c in df.columns]].reset_index(drop=True)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_stock_signals_uses_sepa_columns -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): 銘柄需給シグナル compute_stock_signals(sepaレンジ再利用)"
```

---

### Task 8: ランキング + テーマ内順位 `compute_rankings`

値上がり/値下がり/出来高急増/資金フロー/モメンタムの5ランキングを作り、各銘柄に業種内順位 selfRank を付与。

**Files:**
- Modify: `services/rotation_service.py`
- Test: `tests/test_rotation_service.py`

- [ ] **Step 1: Write the failing test**

```python
def test_compute_rankings_self_rank_within_sector():
    sc = pd.DataFrame([
        {"code": "0001", "company_name": "A", "sector": "X",
         "score": 90, "mom_signal": "strong", "latest_volume": 100,
         "avg_volume": 50, "close": 110, "MA25": 100},
        {"code": "0002", "company_name": "B", "sector": "X",
         "score": 70, "mom_signal": "", "latest_volume": 80,
         "avg_volume": 80, "close": 90, "MA25": 100},
        {"code": "0003", "company_name": "C", "sector": "Y",
         "score": 80, "mom_signal": "strong", "latest_volume": 300,
         "avg_volume": 60, "close": 120, "MA25": 100},
    ])
    out = rs.compute_rankings(sc)
    assert set(out.keys()) >= {"momentum", "volume_surge"}
    mom = out["momentum"]
    # 業種X内で score 最上位の 0001 は self_rank=1
    a = mom[mom["code"] == "0001"].iloc[0]
    assert a["self_rank"] == 1
    assert a["self_rank_total"] == 2  # 業種Xは2銘柄
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_rankings_self_rank_within_sector -v`
Expected: FAIL（`has no attribute 'compute_rankings'`）

- [ ] **Step 3: Write minimal implementation**

```python
def _with_self_rank(df: pd.DataFrame, by: str) -> pd.DataFrame:
    df = df.copy()
    df["self_rank"] = (
        df.groupby("sector")[by].rank(ascending=False, method="min").astype(int)
    )
    df["self_rank_total"] = df.groupby("sector")["sector"].transform("size")
    return df


def compute_rankings(stock_cache: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """5種のランキングを返す。各行に業種内順位 self_rank / self_rank_total 付き。

    keys: momentum, volume_surge（値上がり/値下がりは close/MA25 と将来の日次騰落で拡張）
    """
    df = stock_cache.copy()
    df["score"] = pd.to_numeric(df.get("score"), errors="coerce")
    df["vol_ratio"] = (
        pd.to_numeric(df.get("latest_volume"), errors="coerce")
        / pd.to_numeric(df.get("avg_volume"), errors="coerce")
    )
    rankings: Dict[str, pd.DataFrame] = {}
    rankings["momentum"] = _with_self_rank(
        df.sort_values("score", ascending=False), by="score"
    ).reset_index(drop=True)
    rankings["volume_surge"] = _with_self_rank(
        df.sort_values("vol_ratio", ascending=False), by="vol_ratio"
    ).reset_index(drop=True)
    return rankings
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py::test_compute_rankings_self_rank_within_sector -v`
Expected: PASS

- [ ] **Step 5: 全 rotation テストをまとめて確認**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py -v`
Expected: 全 PASS

- [ ] **Step 6: Commit**

```bash
git add services/rotation_service.py tests/test_rotation_service.py
git commit -m "feat(rotation): ランキング5種 compute_rankings(テーマ内順位付き)"
```

---

## Phase 2: 段階2 — JPX 信用データ

### Task 9: extract_pdf.py に `extract_text()` を公開

`fitz` import を集約したまま、抽出ロジックを関数化して他モジュールから再利用可能にする。CLI 挙動は不変。

**Files:**
- Modify: `scripts/extract_pdf.py`

- [ ] **Step 1: Write the failing test**

`tests/test_margin_service.py`:

```python
# -*- coding: utf-8 -*-
"""JPX 信用 PDF パーサの単体テスト。"""
import os

import pandas as pd
import pytest

from services import margin_service


def test_extract_text_is_importable():
    from scripts.extract_pdf import extract_text
    assert callable(extract_text)
```

`scripts/__init__.py` が無い場合は作成（import 可能にするため）:

Run: `touch scripts/__init__.py`

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_margin_service.py::test_extract_text_is_importable -v`
Expected: FAIL（`cannot import name 'extract_text'` または margin_service 不在）

- [ ] **Step 3: Refactor extract_pdf.py**

`scripts/extract_pdf.py` の `main()` 内の PDF オープン〜テキスト連結部分を、次の新関数に切り出す（`main()` はこの関数を呼ぶだけにする）:

```python
def extract_text(pdf_path) -> str:
    """PDF からプレーンテキストを抽出して返す（ページ区切り付き）。

    fitz(PyMuPDF) import はこの関数に集約。他モジュールはこれを import して使う。
    """
    from pathlib import Path
    import fitz  # PyMuPDF

    doc = fitz.open(str(Path(pdf_path)))
    page_texts = []
    for i, page in enumerate(doc):
        if i > 0:
            page_texts.append("\n--- PAGE BREAK ---\n")
        page_texts.append(page.get_text())
    return "".join(page_texts)
```

`main()` の当該ブロックを `full_text = extract_text(pdf_path)` に置換（try/except の ImportError メッセージは main 側に残す）。

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_margin_service.py::test_extract_text_is_importable -v`
Expected: `test_extract_text_is_importable` は PASS（margin_service 不在なら次タスクまで collection error → その場合は Step 1 の `from services import margin_service` を Task 10 まで削除しておき、本タスクでは extract_text の import テストのみに絞る）

- [ ] **Step 5: CLI が壊れていないことを確認**

Run: `.venv/Scripts/python.exe scripts/extract_pdf.py 2>&1 | head -1`
Expected: `Usage: extract_pdf.py <pdf_path> [--save]`（引数無しの usage 表示、クラッシュしない）

- [ ] **Step 6: Commit**

```bash
git add scripts/extract_pdf.py scripts/__init__.py tests/test_margin_service.py
git commit -m "refactor(extract_pdf): extract_text()を公開しfitz importを集約"
```

---

### Task 10: JPX 信用 PDF パーサ `margin_service.parse_margin_text`

抽出済みテキストから銘柄別の売残・買残・前週比をパースし、信用倍率を算出。テストは軽量な抽出済みテキスト fixture を使う。

**Files:**
- Create: `services/margin_service.py`
- Create: `tests/fixtures/margin_sample.txt`
- Test: `tests/test_margin_service.py`

- [ ] **Step 1: サンプル fixture を作成**

実 PDF から抽出したテキストの一部を数銘柄に絞って `tests/fixtures/margin_sample.txt` に保存する。JPX の実レイアウトに合わせた最小サンプル（極洋の実値を含む）:

```
2026/7/17 申込み現在
End-of-week outstanding margin trading by issue
（単位：一株）
貸借銘柄 loan trading issue
B
極洋　普通株式
13010
JP3257200000
2,800
▲ 200
162,700
▲ 700
300
0
2,500
▲ 200
B
日本水産　普通株式
13320
JP3718800000
15,000
1,000
250,000
5,000
2,000
500
13,000
500
```

> **重要**: このサンプルは実 PDF の抽出結果の列順（売残高・前週比・買残高・前週比・一般/制度内訳…）を反映する。実装前に一度 `scripts/extract_pdf.py` で実ファイルを抽出し、実際の行順を確認してこの fixture を正確に作り直すこと（列順が想定と違えば Step 3 のパーサと fixture を合わせて修正）。

- [ ] **Step 2: Write the failing test**

`tests/test_margin_service.py` に追記:

```python
FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "margin_sample.txt")


def test_parse_margin_text_extracts_known_stock():
    with open(FIXTURE, encoding="utf-8") as f:
        text = f.read()
    df = margin_service.parse_margin_text(text)
    kyokuyo = df[df["code"] == "13010"].iloc[0]
    assert kyokuyo["sell_balance"] == 2800
    assert kyokuyo["buy_balance"] == 162700
    # 信用倍率 = 買残 / 売残
    assert kyokuyo["margin_ratio"] == pytest.approx(162700 / 2800, rel=1e-4)


def test_parse_margin_text_raises_on_garbage():
    with pytest.raises(ValueError):
        margin_service.parse_margin_text("這は信用PDFではないテキスト")
```

- [ ] **Step 3: Write minimal implementation**

`services/margin_service.py`:

```python
# -*- coding: utf-8 -*-
"""JPX「銘柄別信用取引週末残高」PDF の取得・パース・アーカイブ。

PDF テキスト抽出は scripts/extract_pdf.extract_text() に委譲（fitz を直接 import しない）。
"""
from __future__ import annotations

import os
import re
from typing import Optional

import pandas as pd

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MARGIN_DIR = os.path.join(_ROOT, "data", "margin")

_CODE_RE = re.compile(r"^\d{4}0$")          # 5桁コード（末尾0）
_ISIN_RE = re.compile(r"^JP\d{10}$")
_NUM_RE = re.compile(r"^[▲△+\-]?\s*[\d,]+$")


def _to_int(token: str) -> int:
    t = token.replace(",", "").replace("▲", "-").replace("△", "-").replace("+", "").strip()
    return int(t)


def parse_margin_text(text: str) -> pd.DataFrame:
    """抽出テキストから銘柄別信用残をパースする。

    各銘柄ブロック: 銘柄名 → コード(5桁) → ISIN → 売残高 → 前週比 →
    買残高 → 前週比 → (一般/制度の内訳が続く)
    Returns 列: code, sell_balance, sell_wow, buy_balance, buy_wow, margin_ratio
    """
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    rows = []
    i = 0
    while i < len(lines):
        if _CODE_RE.match(lines[i]) and i + 1 < len(lines) and _ISIN_RE.match(lines[i + 1]):
            code = lines[i]
            # ISIN の次から数値が4つ: 売残・前週比・買残・前週比
            nums = []
            j = i + 2
            while j < len(lines) and len(nums) < 4 and _NUM_RE.match(lines[j]):
                nums.append(_to_int(lines[j]))
                j += 1
            if len(nums) == 4:
                sell, sell_wow, buy, buy_wow = nums
                ratio = (buy / sell) if sell else float("nan")
                rows.append({
                    "code": code, "sell_balance": sell, "sell_wow": sell_wow,
                    "buy_balance": buy, "buy_wow": buy_wow, "margin_ratio": ratio,
                })
            i = j
        else:
            i += 1
    if not rows:
        raise ValueError("信用残データを1件も抽出できませんでした（フォーマット変更の可能性）")
    return pd.DataFrame(rows)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_margin_service.py -v`
Expected: 全 PASS

> 実 fixture の列順が異なった場合は、Step 1 で確認した実際の順に合わせて `nums` の unpack 順とサンプルを修正。

- [ ] **Step 5: Commit**

```bash
git add services/margin_service.py tests/test_margin_service.py tests/fixtures/margin_sample.txt
git commit -m "feat(margin): JPX信用PDFパーサ parse_margin_text(信用倍率算出)"
```

---

### Task 11: 信用データのDL・アーカイブ・最新ロード

PDF ダウンロード、`data/margin/{YYYYMMDD}.parquet` への保存、最新アーカイブのロードを実装。DL はネットワーク依存のためユニットテストせず、ロード/最新選択のみテスト。

**Files:**
- Modify: `services/margin_service.py`
- Test: `tests/test_margin_service.py`

- [ ] **Step 1: Write the failing test**

```python
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/Scripts/python.exe -m pytest tests/test_margin_service.py::test_load_latest_margin_picks_newest -v`
Expected: FAIL（`has no attribute 'load_latest_margin'`）

- [ ] **Step 3: Write minimal implementation**

`services/margin_service.py` に追記:

```python
import glob


def load_latest_margin() -> Optional[pd.DataFrame]:
    """data/margin/ の最新（日付最大）アーカイブを返す。無ければ None。"""
    if not os.path.isdir(MARGIN_DIR):
        return None
    files = sorted(glob.glob(os.path.join(MARGIN_DIR, "*.parquet")))
    if not files:
        return None
    return pd.read_parquet(files[-1])


def save_margin_archive(df: pd.DataFrame, as_of_yyyymmdd: str) -> str:
    """信用残 DataFrame を data/margin/{YYYYMMDD}.parquet に保存し、パスを返す。"""
    os.makedirs(MARGIN_DIR, exist_ok=True)
    path = os.path.join(MARGIN_DIR, f"{as_of_yyyymmdd}.parquet")
    df.to_parquet(path, index=False)
    return path


def download_margin_pdf(as_of_yyyymmdd: str, dest_path: str) -> str:
    """JPX 週次信用 PDF をダウンロードして dest_path に保存。

    URL 規則: .../margin/tvdivq0000001rnl-att/syumatsu{YYYYMMDD}00.pdf
    ※ 恒久運用は Streamlit「データ更新」から呼ぶ。SSL 失効チェックは無効化。
    """
    import urllib.request
    import ssl

    url = (
        "https://www.jpx.co.jp/markets/statistics-equities/margin/"
        f"tvdivq0000001rnl-att/syumatsu{as_of_yyyymmdd}00.pdf"
    )
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, context=ctx, timeout=60) as r:
        data = r.read()
    with open(dest_path, "wb") as f:
        f.write(data)
    return dest_path
```

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/Scripts/python.exe -m pytest tests/test_margin_service.py -v`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add services/margin_service.py tests/test_margin_service.py
git commit -m "feat(margin): 週次アーカイブ保存・最新ロード・PDF DL"
```

---

## Phase 3: UI

### Task 12: ページ骨格 — 市況サマリー + キャッシュラッパー

Streamlit ページを作り、データロードとサマリー（テーマ温度ゲージ + KPI + データ基準日）を表示。

**Files:**
- Create: `pages/9_sector_rotation.py`

- [ ] **Step 1: ページ骨格を実装**

`pages/9_sector_rotation.py`:

```python
# -*- coding: utf-8 -*-
"""セクター回転検知ダッシュボード（KabuTrend /trend 参考・自前データ）。"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services import batch_service, rotation_service, margin_service

st.set_page_config(page_title="セクター回転", layout="wide")
st.title("セクター回転検知")


@st.cache_data(ttl=3600)
def _load_data():
    prices = pd.read_parquet(batch_service.PRICES_PATH)
    sc = batch_service.load_cache()
    return prices, sc


@st.cache_data(ttl=3600)
def _sector_daily(prices, sector_map):
    return rotation_service.load_sector_daily(prices, sector_map)


prices, sc = _load_data()
if sc is None or prices.empty:
    st.warning("stock_cache または prices が空です。先に『データ更新』を実行してください。")
    st.stop()

sector_map = dict(zip(sc["code"], sc["sector"]))
data_date = str(pd.to_datetime(prices["Date"]).max().date())
st.caption(f"データ基準日: {data_date}")

sd = _sector_daily(prices, sector_map)

# サマリー用: 各業種の期間リターン(月)と最新breadth
summary = rotation_service.compute_freshness(sd)[["sector", "month_return"]].copy()
summary = summary.rename(columns={"month_return": "period_return"})
latest_breadth = sd.sort_values("Date").groupby("sector")["up_ratio"].last()
summary["breadth"] = summary["sector"].map(latest_breadth)
temp = rotation_service.compute_theme_temperature(summary)

col1, col2, col3 = st.columns(3)
with col1:
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=temp,
        title={"text": "テーマ温度"},
        gauge={"axis": {"range": [0, 100]}},
    ))
    fig.update_layout(height=220, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)
with col2:
    adv = float((summary["period_return"] > 0).mean()) * 100.0
    st.metric("上昇業種比率", f"{adv:.1f}%")
with col3:
    surge = rotation_service.compute_volume_surge(sd)
    st.metric("出来高急増業種数", int((surge["turnover_ratio"] >= 1.5).sum()))
```

- [ ] **Step 2: 起動確認**

Run:
```bash
.venv/Scripts/python.exe -c "import ast; ast.parse(open('pages/9_sector_rotation.py', encoding='utf-8').read()); print('syntax OK')"
```
Expected: `syntax OK`

- [ ] **Step 3: Streamlit で目視確認**

Run: `.venv/Scripts/streamlit.exe run app.py --server.port 8502`
ブラウザで「セクター回転」ページを開き、テーマ温度ゲージ・KPI・データ基準日が表示され、例外が出ないことを確認。確認後 Ctrl+C。

- [ ] **Step 4: Commit**

```bash
git add pages/9_sector_rotation.py
git commit -m "feat(ui): セクター回転ページ骨格(温度ゲージ+KPI+基準日)"
```

---

### Task 13: セクションUI + ランキングタブ + ドリルダウン遷移

出来高急増・鮮度・資金流入・信用需給・ランキング・ドリルダウン（`switch_page` 遷移）を追加。

**Files:**
- Modify: `pages/9_sector_rotation.py`

- [ ] **Step 1: 各セクションを追記**

`pages/9_sector_rotation.py` の末尾に追記:

```python
st.divider()

# ── 出来高急増 ──
st.subheader("出来高急増")
st.dataframe(surge.head(15), use_container_width=True)

# ── 鮮度 ──
st.subheader("鮮度（資金の新旧）")
fresh = rotation_service.compute_freshness(sd)
c_rise, c_win, c_fall = st.columns(3)
with c_rise:
    st.caption("上昇中（新しく来た）")
    st.dataframe(fresh[fresh["category"] == "rising"][["sector", "week_return", "rank_delta"]], use_container_width=True)
with c_win:
    st.caption("勝ち続け")
    st.dataframe(fresh[fresh["category"] == "winning"][["sector", "week_return", "month_return"]], use_container_width=True)
with c_fall:
    st.caption("失速")
    st.dataframe(fresh[fresh["category"] == "falling"][["sector", "week_return", "rank_delta"]], use_container_width=True)

# ── 資金流入 ──
st.subheader("資金流入スコア")
flow = rotation_service.compute_fund_flow(sd)
st.dataframe(flow.head(15), use_container_width=True)

# ── 信用需給（段階2・アーカイブがあれば） ──
margin_df = margin_service.load_latest_margin()
if margin_df is not None:
    st.subheader("信用需給（JPX週次）")
    st.dataframe(
        margin_df.sort_values("margin_ratio", ascending=False).head(15),
        use_container_width=True,
    )

# ── ランキング ──
st.subheader("ランキング")
rankings = rotation_service.compute_rankings(sc)
tabs = st.tabs(["モメンタム", "出来高急増(銘柄)"])
with tabs[0]:
    st.dataframe(
        rankings["momentum"].head(30)[["code", "company_name", "sector", "score", "self_rank", "self_rank_total"]],
        use_container_width=True,
    )
with tabs[1]:
    st.dataframe(
        rankings["volume_surge"].head(30)[["code", "company_name", "sector", "vol_ratio", "self_rank"]],
        use_container_width=True,
    )

# ── ドリルダウン ──
st.divider()
st.subheader("業種ドリルダウン")
signals = rotation_service.compute_stock_signals(sc)
sectors = sorted(signals["sector"].dropna().unique())
sel = st.selectbox("業種を選択", sectors)
members = signals[signals["sector"] == sel].sort_values("ma25_dev_pct", ascending=False)
st.dataframe(
    members[["code", "company_name", "close", "RSI", "ma25_dev_pct", "from_52w_high_pct", "new_high"]].head(30),
    use_container_width=True,
)

code_to_open = st.text_input("詳細を開く銘柄コード（5桁）", "")
if st.button("銘柄詳細へ") and code_to_open:
    st.session_state["selected_code"] = code_to_open.strip()
    st.switch_page("pages/2_stock_detail.py")
```

- [ ] **Step 2: 構文確認**

Run:
```bash
.venv/Scripts/python.exe -c "import ast; ast.parse(open('pages/9_sector_rotation.py', encoding='utf-8').read()); print('syntax OK')"
```
Expected: `syntax OK`

- [ ] **Step 3: Streamlit で目視確認**

Run: `.venv/Scripts/streamlit.exe run app.py --server.port 8502`
「セクター回転」ページで全セクションが表示され、ランキングタブが切替でき、ドリルダウンで業種選択→構成銘柄表示、「銘柄詳細へ」ボタンで 2_stock_detail に遷移し正しい銘柄が開くことを確認。確認後 Ctrl+C。

- [ ] **Step 4: 全テスト最終確認**

Run: `.venv/Scripts/python.exe -m pytest tests/test_rotation_service.py tests/test_margin_service.py -v`
Expected: 全 PASS

- [ ] **Step 5: Commit**

```bash
git add pages/9_sector_rotation.py
git commit -m "feat(ui): 回転セクション・ランキングタブ・ドリルダウン遷移"
```

---

## Phase 4: 統合確認

### Task 14: 実データでのスモーク + 信用パイプライン疎通

**Files:** なし（手動確認）

- [ ] **Step 1: 実 JPX PDF で信用パイプラインを1回通す**

Run:
```bash
.venv/Scripts/python.exe -c "
from services import margin_service as m
from scripts.extract_pdf import extract_text
import os
os.makedirs('data/margin', exist_ok=True)
pdf = 'data/margin/_tmp_syumatsu.pdf'
m.download_margin_pdf('20260717', pdf)
df = m.parse_margin_text(extract_text(pdf))
print('parsed rows:', len(df))
path = m.save_margin_archive(df, '20260717')
print('saved:', path)
os.remove(pdf)
"
```
Expected: `parsed rows: 2000+`、`saved: data/margin/20260717.parquet`。行数が極端に少ない/0 ならフォーマット変更を疑い Task 10 のパーサを実テキストで再調整。

- [ ] **Step 2: Streamlit で信用需給セクションが出ることを確認**

Run: `.venv/Scripts/streamlit.exe run app.py --server.port 8502`
「信用需給（JPX週次）」セクションが表示されることを確認。

- [ ] **Step 3: 最終コミット（アーカイブは gitignore 済みなのでコード差分のみ）**

```bash
git status  # data/margin/ が追跡されていないことを確認
git commit --allow-empty -m "chore: セクター回転ダッシュボード統合確認完了"
```

---

## 完了条件

- [ ] `tests/test_rotation_service.py` 全 PASS
- [ ] `tests/test_margin_service.py` 全 PASS
- [ ] Streamlit「セクター回転」ページが実データで例外なく表示
- [ ] 段階1（回転4指標 + ランキング + 銘柄需給 + ドリルダウン遷移）が動作
- [ ] 段階2（JPX信用PDF → margin.parquet → 信用需給セクション）が疎通
- [ ] `requirements.txt` に pymupdf、`.gitignore` に data/margin/ が入っている
