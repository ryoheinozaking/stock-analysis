# バリュー株モード診断：実装引き継ぎ書

## 背景・目的

成長株モード診断（Phase 1〜3）で以下が判明：

1. 成長株 filter は universe 平均が TOPIX を **-1.4%** 下回る（filter drag）
2. 成長株 stock-picking α（Top 5）は **+2.6% / 60日** とボーダー有意で存在
3. TPX α は filter drag と picking α の差し引き（+1.1%）にすぎなかった

CLAUDE.md には「バリューモード Top 20: TPX α +18.14% / 勝率 86.1%」と記載されているが、これは**成長モードと同じ古い測定方法**による数字。本当に強いのか、universe α / filter drag の分解で再検証する必要がある。

**本診断のゴール**：バリューモードの真の実力を以下 3 つに分解して測る。

```
TPX α  =  filter drag  +  stock-picking α
```

これにより、3 シナリオのいずれかを特定する：

| シナリオ | universe α (picking) | filter (vs TOPIX) | 戦略判断 |
|---|---|---|---|
| **A. 真にスキルあり** | > +3% | 中立〜やや有利 | バリューモード本格運用、governance 拡張 |
| **B. filter premium 主体** | +1〜3% | 大幅プラス（バリュー premium 取り） | 維持、ただし期待値は控えめに |
| **C. 実質ノイズ** | < +1% | プラス | TOPIX ETF の方が良い、個別株戦略を放棄検討 |

---

## 設計方針：成長株診断の構造を流用

`services/diagnose_growth_service.py` と `diagnose_growth.py` を**ほぼコピーして mode を value に切り替える**形で実装する。これによりロジックの整合性を担保し、結果の比較可能性も確保する。

新規ファイル 2 つ：
- `services/diagnose_value_service.py`
- `diagnose_value.py`

出力先：
- `data/diagnose_value/*.csv`

既存の `services/diagnose_growth_service.py` と `diagnose_growth.py` は**触らない**（成長株診断は参照用に残す）。

---

## 実装タスク

### タスク 1: `services/diagnose_value_service.py` を新規作成

`services/diagnose_growth_service.py` を**そのままコピー**してから、以下の差分を適用する。

#### 1.1 ヘッダ・docstring 修正

```python
# -*- coding: utf-8 -*-
"""
バリュー株モード診断サービス（Rank IC・Top N α・重みスイープ・universe α）

成長株モード診断（diagnose_growth_service.py）と同じ規律を、
バリュー株モードに適用する。

CLAUDE.md に記載されたバリューモードの「TPX α=+18.14%」が真の選別スキルか、
filter premium（バリュー universe が TOPIX に勝つ分）の取り分かを分解する。

主要関数:
  run_diagnosis()        -- 全診断を実行（メインエントリ）
  calc_rank_ic()         -- 指標別 Spearman IC を集計
  calc_alpha_by_topn()   -- Top N 別 forward α を計算（TPX/universe の両軸）
  sweep_weights()        -- Funda/Tech 重みスイープ
  sweep_funda_variants() -- ファンダ・バリアント × 重みスイープ

データソースは pipeline_service・backtest_value_service と共通の
prices.parquet / fins_cache.parquet / stock_cache.parquet を使用。
"""
```

#### 1.2 出力ディレクトリ変更

```python
_OUT_DIR = os.path.join(_ROOT, "data", "diagnose_value")
```

#### 1.3 診断対象ファクター変更

```python
# 診断対象ファクター（バリューモード仕様）
FACTORS_FUNDA = [
    # コア value
    "PBR", "PER", "psr",
    # quality
    "ROE", "op_margin", "equity_ratio",
    # growth（参考、IC ほぼ 0 のはず）
    "rev_growth", "profit_growth", "eps_growth",
]
FACTORS_SCORE = ["funda_score", "tech_score", "total_score", "value_funda_v2"]
FACTORS_ALL   = FACTORS_FUNDA + FACTORS_SCORE
```

#### 1.4 `run_growth_snapshot` を `run_value_snapshot` にリネーム＋mode 切替

関数名と中身の `mode="growth"` を `mode="value"` に変更：

```python
def run_value_snapshot(
    as_of_date:   str,
    prices_df:    pd.DataFrame,
    fins_fy:      pd.DataFrame,
    stock_meta:   pd.DataFrame,
    top_n:        int = 20,
    forward_days: int = 60,
    progress_cb:  Optional[Callable] = None,
) -> Dict:
    """
    as_of_date 時点のバリュー株パイプラインを再現し、
    forward_days 後のリターン付き scored DataFrame を返す。
    （以下 docstring は同じ）
    """
    ...
    # ハードフィルタ呼び出しを value に変更
    filtered = apply_hard_filter(snap_df, fins_metrics, mode="value")
    ...
    # スコア計算も value
    scored = calc_funda_score(filtered, mode="value")
    scored = calc_tech_scores(scored, prices_past, mode="value")
    scored = calc_total_score(scored)
    ...
```

`run_diagnosis` 内の `run_growth_snapshot(...)` 呼び出しも `run_value_snapshot(...)` に変更。

#### 1.5 `_add_growth_funda_v2` を `_add_value_funda_v2` にリネーム＋定義変更

成長モード版は「PBR/PER/PSR を順方向（高いほど高得点）」だったが、バリューモード版は**バリュー premium 検証用**として「**PBR のみで純粋にスコア**」する。

```python
def _add_value_funda_v2(snapshot_results: List[Dict]) -> None:
    """
    各スナップショットの scored DataFrame に `value_funda_v2` 列を追加（破壊的）。

    定義（バリューモードの実験的 funda スコア）:
      - PBR の percentile rank（低いほど高得点 = 普通の value 方向）を 1.0 倍
      - PER, PSR は使わない（PBR との冗長性確認）
      - quality / growth は混ぜない

    意図: 既存 funda_score（PBR 50pt + PSR 30pt + PER 10pt + op_margin 10pt）と、
          PBR-only スコアの IC を比較。PBR 単独で十分か、合成の意味があるかを判定。
    """
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        if "PBR" in df.columns:
            # 低いほど高得点 = ascending rank の逆を取る
            pbr_rank = df["PBR"].rank(pct=True, ascending=True, na_option="keep")
            df["value_funda_v2"] = 1.0 - pbr_rank   # 低 PBR ほど 1.0 に近い
```

`run_diagnosis` 内の `_add_growth_funda_v2(...)` 呼び出しも `_add_value_funda_v2(...)` に変更。

#### 1.6 `sweep_funda_variants` のデフォルト変更

```python
def sweep_funda_variants(
    snapshot_results: List[Dict],
    prices_df:        pd.DataFrame,
    forward_days:     int,
    variants:         Optional[List[str]] = None,
    weights:          Optional[List[Tuple[float, float]]] = None,
    top_ns:           Optional[List[int]] = None,
    benchmark_code:   str = BENCHMARK_CODE,
) -> pd.DataFrame:
    """..."""
    if variants is None:
        variants = ["funda_score", "value_funda_v2"]   # ← v2 を value 版に
    ...
```

その他の関数（`calc_alpha_by_topn`, `sweep_weights`, `_precompute_universe_returns` 等）は**そのまま流用**。

#### 1.7 戻り値の docstring 修正

`run_diagnosis` の docstring で「成長株パイプライン」→「バリュー株パイプライン」に変更。

#### 1.8 universe α / filter drag を最初から組み込んでおく

成長株版 Phase 3 で追加した `_precompute_universe_returns` および各 sweep 関数の `universe_mean` / `universe_alpha` カラムは、**コピー時点ですでに含まれているので追加作業不要**。

### タスク 2: `diagnose_value.py` を新規作成

`diagnose_growth.py` を**そのままコピー**してから、以下の差分を適用する。

#### 2.1 docstring・メッセージ変更

ファイル冒頭の docstring 内の「成長株モード」→「バリュー株モード」、出力先 `data/diagnose_growth/` → `data/diagnose_value/` に置換。

#### 2.2 import 変更

```python
from services.diagnose_value_service import run_diagnosis
```

#### 2.3 表示メッセージの「成長株」を「バリュー株」に置換

`print_summary` 内の以下を変更：

```python
print(f"  バリュー株モード Rank IC 診断")   # was: 成長株モード
```

#### 2.4 「現状」マーカーの調整

成長株モード版の重みスイープには `<- 現状` フラグが `(0.6, 0.4)` で点灯するロジックがある：

```python
flag = "  <- 現状" if (abs(fw_val - 0.6) < 1e-9 and abs(tw_val - 0.4) < 1e-9) else ""
```

バリューモードの現状重みは `pipeline_service.py` の `calc_total_score` を確認して調整する（おそらく `(0.6, 0.4)` だが、念のため確認）。同じであれば変更不要。

#### 2.5 「参考 [バリューモード実績]」セクションの差し替え

成長株モード版にあるバリューモード比較セクションは、バリューモード自身の診断では不要なので**削除**する。代わりに以下を表示：

```python
print(f"\n  参考 [CLAUDE.md 記載のバリューモード旧バックテスト]:")
print(f"  Top 20 平均リターン:    +37.67%")
print(f"  TPX α:                 +18.14%   <- これが本物か検証中")
print(f"  勝率:                   86.1%")
print(f"  期間:                   37 月次スナップショット")
```

#### 2.6 ARGV 変更

`argparse.ArgumentParser` の説明文：

```python
description="バリュー株モード診断（Rank IC / Top N α / 重みスイープ / universe α）"
```

#### 2.7 コマンドライン helper

`print` の冒頭に出るタイトル：

```python
print(f"バリュー株モード診断 開始")
```

### タスク 3: ベースケース（CLAUDE.md と同条件）の追加診断

CLAUDE.md のバリューモード過去バックテストは「**Top 20 / 12 ヶ月保有**」だった。これと比較可能にするため、**追加で `--fwd 252` 相当の長期保有も測定**する。

`diagnose_value.py` の CLI 引数は既に `--fwd` で柔軟に変更可能なので、以下の 2 回の実行を完了条件に含める：

```bash
# 短期（成長株診断と比較可能）
.venv/Scripts/python.exe diagnose_value.py --fwd 60 --top 20

# 長期（CLAUDE.md の旧バックテスト「Top 20 / 12 ヶ月」と比較可能）
.venv/Scripts/python.exe diagnose_value.py --fwd 250 --top 20
```

長期版の出力 CSV は上書きされるので、長期実行前に短期版の CSV を退避する：

```bash
mv data/diagnose_value/ic_by_factor.csv         data/diagnose_value/ic_by_factor_fwd60.csv
mv data/diagnose_value/alpha_by_topn.csv        data/diagnose_value/alpha_by_topn_fwd60.csv
mv data/diagnose_value/weight_sweep.csv         data/diagnose_value/weight_sweep_fwd60.csv
mv data/diagnose_value/funda_variant_sweep.csv  data/diagnose_value/funda_variant_sweep_fwd60.csv
```

その後 fwd 250 を実行し、出力 CSV を `*_fwd250.csv` にリネーム：

```bash
mv data/diagnose_value/ic_by_factor.csv         data/diagnose_value/ic_by_factor_fwd250.csv
mv data/diagnose_value/alpha_by_topn.csv        data/diagnose_value/alpha_by_topn_fwd250.csv
mv data/diagnose_value/weight_sweep.csv         data/diagnose_value/weight_sweep_fwd250.csv
mv data/diagnose_value/funda_variant_sweep.csv  data/diagnose_value/funda_variant_sweep_fwd250.csv
```

---

## 完了条件

1. `.venv/Scripts/python.exe diagnose_value.py --fwd 60 --top 20 --snapshots 1` がエラーなく完走する。
2. 短期診断 `--fwd 60 --top 20`（全スナップショット）が完走し、CSV 4 種が生成される。CSV を `*_fwd60.csv` にリネーム。
3. 長期診断 `--fwd 250 --top 20`（全スナップショット）が完走し、CSV 4 種が生成される。CSV を `*_fwd250.csv` にリネーム。
4. 出力に以下が含まれる：
   - Rank IC 表に PBR、PER、PSR、ROE、value_funda_v2 などの行
   - Top N alpha 表に `univ` と `a(univ)` 列が表示
   - 重みスイープに `a(univ)` 列が表示
   - variant sweep に `a(univ)` 列が表示され、`a(univ)` でソート

---

## 実装上の注意

- **既存ファイル `services/diagnose_growth_service.py` および `diagnose_growth.py` は触らない**。
- **新規ファイル 2 つだけ作成**（`services/diagnose_value_service.py`、`diagnose_value.py`）。
- **cp932 文字を使わない**（罫線は `-` `=`、矢印は `<<` `<-`、ギリシャ文字不可、`α` は `a` または `alpha`）。
- 各 funda 指標の percentile rank の方向（高いほど良い vs 低いほど良い）は流用するロジックそのまま（`apply_hard_filter` と `calc_funda_score` が mode 別に正しく処理する）。
- `_calc_fwd_returns` は forward_days を引数で受けるので、`--fwd 250` でも問題なく動作するはず。`generate_monthly_snapshots` は `latest_valid = p_max - pd.Timedelta(days=forward_days)` で月末リストを絞るので、長期にすると有効スナップショット数が減る点に注意（fwd=250 だと 250 日分減る → 約 8 ヶ月減）。
- 長期実行は時間がかかる（全 60 銘柄 ×〜30 スナップショット ×〜100 銘柄/snap で計算量が多い）。背景実行＋進捗ログを確認しながら待つこと。

---

## 実行コマンド

```bash
cd C:\Users\ryohei\stock_analysis

# Step 1: 単一スナップショットで動作確認
.venv/Scripts/python.exe diagnose_value.py --fwd 60 --top 20 --snapshots 1

# Step 2: 短期（60日）診断
.venv/Scripts/python.exe diagnose_value.py --fwd 60 --top 20
# 完走後、CSV を *_fwd60.csv にリネーム

# Step 3: 長期（250日 ≒ 12ヶ月）診断
.venv/Scripts/python.exe diagnose_value.py --fwd 250 --top 20
# 完走後、CSV を *_fwd250.csv にリネーム
```

---

## 完了後に Opus に報告すべきこと

短期（fwd 60）と長期（fwd 250）の両方について、以下を貼ってください：

### A. 短期（fwd 60）

1. **Rank IC 表 全行**（特に PBR、PER、PSR、ROE、rev_growth の mean_ic と t_stat）
2. **Top N alpha 表 全行**（top_n=3, 5, 7, 10, 15, 20, 30 の各行で a(TPX) と a(univ) を比較）
3. **重みスイープ 全行**（特に 0.5/0.5, 0.6/0.4 の a(univ)）
4. **variant sweep 上位 5 行**（universe_alpha でソート後）
5. **alpha_by_topn_fwd60.csv の `topix_mean` と `universe_mean` 列**（filter drag = universe_mean - topix_mean を見る）

### B. 長期（fwd 250）

6. **Rank IC 表 全行**（短期と方向が同じか確認）
7. **Top N alpha 表 全行**（特に top_n=20 で CLAUDE.md の旧バックテスト「TPX α +18.14%」と整合するか）
8. **alpha_by_topn_fwd250.csv の `topix_mean`, `universe_mean`, `alpha`, `universe_alpha`**

これらを基に Opus は以下を判断：

- バリュー filter の drag/premium（universe vs TOPIX の差）
- 真の stock-picking α（universe α）
- CLAUDE.md の「+18% TPX α」の正体（picking スキルか、filter premium か、生存バイアスか）
- バリューモードを継続するか、TOPIX ETF へ移行するか、新戦略を構築するか の戦略決定

---

## トラブルシューティング

- `apply_hard_filter` で「フィルタ通過なし」が連続する場合、PBR や equity_ratio の閾値が厳しすぎる可能性。snap_df に対象指標が NaN だらけでないか確認。
- value_funda_v2 列が IC 表に出ない場合、`_add_value_funda_v2` の呼び出しが `calc_rank_ic` の前にあるか確認。
- universe_alpha が全行 None になる場合、`_precompute_universe_returns` が空辞書を返している（has_fwd_data=True の銘柄が 0）。スナップショットの fwd 計算が機能しているか確認。
