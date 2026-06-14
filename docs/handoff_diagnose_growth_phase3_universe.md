# 成長株モード診断 Phase 3：ユニバース α 計測の追加

## 背景・目的

Phase 1/2 の診断は「Top N の平均リターン vs TOPIX」で α を計測してきたが、TOPIX は成長株戦略のベンチマークとして以下の理由で不適切と判明：

1. **スタイル不一致**：TOPIX は大型・バリュー寄り、成長株モードは中小型・成長寄り → α がスタイル要因で振れる
2. **TOPIX Growth 250 も不適切**：超小型・赤字企業中心で敵が弱すぎ、α が誇大評価される
3. **正しい評価軸**：「フィルタを通過したユニバース全体の equal-weight 平均リターン」と比較すべき

→ これにより**フィルタ自体の効果を控除し、純粋な stock-picking スキルだけ**を測れる。

```
真の stock-picking α = (Top N の平均リターン) - (フィルタ通過全銘柄の equal-weight 平均リターン)
```

**Phase 2A 再設計の効果を正しく評価するために、診断にユニバース α 計算を追加する。**

---

## 既存ファイル

- 診断本体: `services/diagnose_growth_service.py`
- CLI: `diagnose_growth.py`
- 出力: `data/diagnose_growth/*.csv`

これらを**最小改修**で拡張する（新規ファイル作成は不要）。

---

## 実装タスク

### タスク 1: `_precompute_universe_returns` 関数を追加

**ファイル**: `services/diagnose_growth_service.py`

`_precompute_bench_returns` 関数の**直下**に追加：

```python
def _precompute_universe_returns(snapshot_results: List[Dict]) -> Dict[str, float]:
    """
    各スナップショットの「フィルタ通過銘柄 equal-weight 平均 forward リターン」を返す。

    has_fwd_data=True の銘柄全件の return_pct の単純平均。
    これは「stock-picking しなかった場合のユニバース・ベースライン」となる。

    Returns
    -------
    {as_of_date: universe_mean_return_pct}
    """
    out: Dict[str, float] = {}
    for r in snapshot_results:
        if "scored" not in r:
            continue
        valid = r["scored"][r["scored"]["has_fwd_data"]]
        if len(valid) > 0:
            out[r["as_of"]] = float(valid["return_pct"].mean())
    return out
```

### タスク 2: `calc_alpha_by_topn` にユニバース α を追加

**ファイル**: `services/diagnose_growth_service.py` の `calc_alpha_by_topn`

#### 2.1 関数冒頭でユニバース・マップを取得

`bench_map = _precompute_bench_returns(...)` の**直後**に追加：

```python
universe_map = _precompute_universe_returns(snapshot_results)
```

#### 2.2 ループ内でユニバース・リターンを蓄積

`bench_rets: List[float] = []` の**直後**に追加：

```python
universe_rets: List[float] = []
```

そして、`bench_rets.extend([bret] * len(top_valid))` の**直後**に追加：

```python
uret = universe_map.get(r["as_of"], np.nan)
if not np.isnan(uret):
    universe_rets.extend([uret] * len(top_valid))
```

#### 2.3 結果行にユニバース α を追加

既存の `rows.append({...})` に**列を追加**する。`alpha` キーの**直後**に：

```python
                "universe_mean": round(float(np.nanmean(universe_rets)), 2)
                                 if universe_rets else None,
                "universe_alpha": round(mean_ret - float(np.nanmean(universe_rets)), 2)
                                  if universe_rets else None,
```

「データなし」の rows.append にも以下を追加：

```python
            rows.append({"top_n": top_n, "n": 0, "mean_return": None,
                         "median_return": None, "win_rate": None,
                         "min": None, "max": None,
                         "topix_mean": None, "alpha": None,
                         "universe_mean": None, "universe_alpha": None})
```

### タスク 3: `sweep_weights` にユニバース α を追加

**ファイル**: `services/diagnose_growth_service.py` の `sweep_weights`

タスク 2 と同じパターン：

1. `bench_map = _precompute_bench_returns(...)` の直後に `universe_map = _precompute_universe_returns(snapshot_results)` を追加
2. `bench_rets: List[float] = []` の直後に `universe_rets: List[float] = []` を追加
3. `bench_rets.extend([bret] * len(top_valid))` の直後に universe 用の同じパターンを追加
4. `rows.append({...})` に `universe_mean` / `universe_alpha` キーを追加（データなし rows.append にも）

### タスク 4: `sweep_funda_variants` にユニバース α を追加

**ファイル**: `services/diagnose_growth_service.py` の `sweep_funda_variants`

タスク 2 と同じパターン（タスク 3 と同様）：

1. `bench_map = ...` の直後に `universe_map = ...`
2. `bench_rets: List[float] = []` の直後に `universe_rets: List[float] = []`
3. `bench_rets.extend(...)` の直後に universe extend
4. `rows.append({...})` に `universe_mean` / `universe_alpha` キー追加（データなし rows.append にも）

### タスク 5: ソート基準を `universe_alpha` に変更

**ファイル**: `services/diagnose_growth_service.py` の `sweep_funda_variants` の戻り値

現状：

```python
return (pd.DataFrame(rows)
          .sort_values("alpha", ascending=False, na_position="last")
          .reset_index(drop=True))
```

修正後：

```python
return (pd.DataFrame(rows)
          .sort_values("universe_alpha", ascending=False, na_position="last")
          .reset_index(drop=True))
```

理由：variant sweep の目的は「stock-picking が効く設定を見つける」こと。フィルタ効果を控除した universe_alpha でソートすべき。

### タスク 6: CLI 出力にユニバース α 列を追加

**ファイル**: `diagnose_growth.py` の `print_summary`

#### 6.1 W を 78 に拡張

```python
W = 78
```

#### 6.2 Top N alpha 表に列を追加

現状：

```python
    print(f"  {'top_n':>6}  {'mean_ret':>9}  {'topix':>7}  {'alpha':>7}"
          f"  {'win%':>6}  {'n':>5}")
    print(f"  {'-'*55}")

    for _, row in alpha_df.iterrows():
        flag = " << 現在" if int(row["top_n"]) == top_n else ""
        print(
            f"  {int(row['top_n']):>6}  {_fmt_pct(row.get('mean_return')):>9}"
            f"  {_fmt_pct(row.get('topix_mean')):>7}"
            f"  {_fmt_pct(row.get('alpha')):>7}"
            f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
            f"  {int(row.get('n', 0)):>5}{flag}"
        )
```

修正後：

```python
    print(f"  {'top_n':>6}  {'mean_ret':>9}  {'topix':>7}  {'a(TPX)':>7}"
          f"  {'univ':>7}  {'a(univ)':>7}  {'win%':>6}  {'n':>5}")
    print(f"  {'-'*72}")

    for _, row in alpha_df.iterrows():
        flag = " << 現在" if int(row["top_n"]) == top_n else ""
        print(
            f"  {int(row['top_n']):>6}  {_fmt_pct(row.get('mean_return')):>9}"
            f"  {_fmt_pct(row.get('topix_mean')):>7}"
            f"  {_fmt_pct(row.get('alpha')):>7}"
            f"  {_fmt_pct(row.get('universe_mean')):>7}"
            f"  {_fmt_pct(row.get('universe_alpha')):>7}"
            f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
            f"  {int(row.get('n', 0)):>5}{flag}"
        )
```

#### 6.3 重みスイープ表に列を追加

現状：

```python
    print(f"  {'funda_w':>8}  {'tech_w':>7}  {'mean_ret':>9}  {'alpha':>7}"
          f"  {'win%':>6}  {'n':>5}")
    print(f"  {'-'*55}")

    for _, row in sweep_df.iterrows():
        ...
        print(
            f"  {fw_val:>8.1f}  {tw_val:>7.1f}"
            f"  {_fmt_pct(row.get('mean_return')):>9}"
            f"  {_fmt_pct(row.get('alpha')):>7}"
            f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
            f"  {int(row.get('n', 0)):>5}{flag}"
        )
```

修正後：

```python
    print(f"  {'funda_w':>8}  {'tech_w':>7}  {'mean_ret':>9}  {'a(TPX)':>7}"
          f"  {'a(univ)':>7}  {'win%':>6}  {'n':>5}")
    print(f"  {'-'*65}")

    for _, row in sweep_df.iterrows():
        ...
        print(
            f"  {fw_val:>8.1f}  {tw_val:>7.1f}"
            f"  {_fmt_pct(row.get('mean_return')):>9}"
            f"  {_fmt_pct(row.get('alpha')):>7}"
            f"  {_fmt_pct(row.get('universe_alpha')):>7}"
            f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
            f"  {int(row.get('n', 0)):>5}{flag}"
        )
```

#### 6.4 variant sweep 表に列を追加

現状：

```python
        print(f"  {'variant':<18} {'fw':>4} {'tw':>4} {'top':>4}"
              f"  {'mean_ret':>9}  {'alpha':>7}  {'win%':>6}  {'n':>5}")
        print(f"  {'-'*65}")
        for _, row in variant_df.head(15).iterrows():
            print(
                f"  {row['variant']:<18} {float(row['funda_w']):>4.1f} {float(row['tech_w']):>4.1f}"
                f" {int(row['top_n']):>4}"
                f"  {_fmt_pct(row.get('mean_return')):>9}"
                f"  {_fmt_pct(row.get('alpha')):>7}"
                f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
                f"  {int(row.get('n', 0)):>5}"
            )
```

修正後：

```python
        print(f"  {'variant':<18} {'fw':>4} {'tw':>4} {'top':>4}"
              f"  {'mean_ret':>9}  {'a(TPX)':>7}  {'a(univ)':>7}  {'win%':>6}  {'n':>5}")
        print(f"  {'-'*78}")
        for _, row in variant_df.head(15).iterrows():
            print(
                f"  {row['variant']:<18} {float(row['funda_w']):>4.1f} {float(row['tech_w']):>4.1f}"
                f" {int(row['top_n']):>4}"
                f"  {_fmt_pct(row.get('mean_return')):>9}"
                f"  {_fmt_pct(row.get('alpha')):>7}"
                f"  {_fmt_pct(row.get('universe_alpha')):>7}"
                f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
                f"  {int(row.get('n', 0)):>5}"
            )
```

#### 6.5 タイトル変更（任意）

variant sweep のタイトルを以下に変更：

```python
        print(f"  [Funda variant x weight x Top N sweep]  universe_alpha 降順 上位15件")
```

---

## 完了条件

1. `.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 20 --snapshots 1` がエラーなく完走する。
2. `.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 20`（全スナップショット）の出力で以下を確認：
   - Top N alpha 表に `univ` と `a(univ)` 列が表示される
   - 重みスイープ表に `a(univ)` 列が表示される
   - variant sweep 表に `a(univ)` 列が表示され、`a(univ)` でソートされている
3. 各 CSV (`alpha_by_topn.csv`, `weight_sweep.csv`, `funda_variant_sweep.csv`) に `universe_mean` と `universe_alpha` 列が含まれる。

---

## 実装上の注意

- **既存関数のシグネチャは変更しない**（呼び出し側の互換性を保つため）。
- **新規ファイルは作らない**。すべて `services/diagnose_growth_service.py` と `diagnose_growth.py` への追記/修正で完結。
- **cp932 文字を使わない**（罫線は `-` `=`、矢印は `<<` `<-`、ギリシャ文字は使わない、`α` は `a` または `alpha` と書く）。
- ユニバース・リターンの計算は **`has_fwd_data=True` の行のみ** を対象とする（top_valid と整合）。
- `np.nanmean` で NaN 安全に計算。

---

## 実行コマンド（確認用）

```bash
cd C:\Users\ryohei\stock_analysis

# 単一スナップショットで動作確認（短時間）
.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 20 --snapshots 1

# 全スナップショット（数分かかる）
.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 20
```

---

## 完了後に Opus に報告すべきこと

CLI 全スナップショット実行の出力から以下を抜粋して貼ってください：

1. **Top N alpha 表 全行**（top_n=3, 5, 7, 10, 15, 20, 30 の各行で a(TPX) と a(univ) を比較）
2. **重みスイープ 全行**（特に 0.5/0.5, 0.3/0.7, 0.0/1.0 の a(univ)）
3. **variant sweep 上位 5 行**（universe_alpha でソート後）
4. 平均ユニバース・リターンの参考値（任意で OK：alpha_by_topn.csv の `universe_mean` のいずれかの行）

これらを基に：
- 既存パイプラインの「真の stock-picking α」を再評価
- Phase 2A 再設計の効果見積もりを更新
- Top N の最終決定（Top 3 vs 5 vs 10）

を行う。
