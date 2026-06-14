# 成長株モード診断 Phase 2 拡張：実装引き継ぎ書

## 背景・目的

成長株モードの Rank IC 診断（59 スナップショット、47 月有効）を実施した結果、現状の funda スコアは構造的に逆効果と判明した：

- **強い負の IC（t<-2）**: PBR (-0.096, t=-3.14)、PER (-0.090, t=-3.01)、PSR (-0.059, t=-2.63)、ROE (-0.046, t=-2.36)、rev_growth (-0.046, t=-2.52)
- **弱い正の IC**: eps_growth (+0.017)、profit_growth (+0.010)、funda_score (+0.011, ノイズ)
- **唯一の有意な正シグナル**: tech_score (+0.043, t=+2.12)
- **Top N**: 5 のみ +1.2% α、10 以上はマイナス
- **重みスイープ**: tech-only 側を測っていない（最大 funda_w=1.0 まで）

**Phase 2A 再設計の前に、設計を一義的に確定するための追加診断を実装する。**

---

## 既存ファイル

- 診断本体: `services/diagnose_growth_service.py`
- CLI: `diagnose_growth.py`
- 出力: `data/diagnose_growth/*.csv`

これらを**最小改修**で拡張する（新規ファイル作成は不要）。

---

## 実装タスク

### タスク 1: Top N の細分化

**ファイル**: `services/diagnose_growth_service.py` の `calc_alpha_by_topn`

**現状**: デフォルト `top_ns = [5, 10, 20, 30, 50]`

**修正後**: デフォルト `top_ns = [3, 5, 7, 10, 15, 20, 30]`

理由：信号が Top 5 に集中している可能性が高く、Top 3 と Top 7 を測ることで「集中度の最適点」を特定する。

### タスク 2: 重みスイープに tech-heavy 側を追加

**ファイル**: `services/diagnose_growth_service.py` の `sweep_weights`

**現状**: デフォルト `weights = [(0.5, 0.5), (0.6, 0.4), (0.7, 0.3), (0.8, 0.2), (1.0, 0.0)]`

**修正後**: デフォルト `weights = [(0.0, 1.0), (0.2, 0.8), (0.3, 0.7), (0.4, 0.6), (0.5, 0.5), (0.6, 0.4), (0.7, 0.3), (1.0, 0.0)]`

理由：funda の IC がほぼゼロなので、tech-only (0.0/1.0) や tech-heavy (0.2/0.8, 0.3/0.7) の方が高 α になる可能性が高い。

### タスク 3: 「growth_funda_v2」（バリュー指標反転スコア）の IC 測定

**ファイル**: `services/diagnose_growth_service.py`

#### 3.1 新規ヘルパー関数を追加

```python
def _add_growth_funda_v2(snapshot_results: List[Dict]) -> None:
    """
    各スナップショットの scored DataFrame に `growth_funda_v2` 列を追加（破壊的）。

    定義（成長モードに最適化した実験的 funda スコア）:
      - PBR / PER / psr を percentile rank で入れる（高いほど高得点 = バリュー指標を反転）
      - eps_growth / profit_growth を percentile rank で入れる（高いほど高得点）
      - 上記の単純平均（0〜1 にスケール）

    意図: PBR/PER/PSR の負 IC は経済的に「成長 universe 内では高 PBR がモメンタム
          シグナル」として解釈できる。これを順方向に取り込んだら IC は改善するか？
    """
    cols_used = ["PBR", "PER", "psr", "eps_growth", "profit_growth"]
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        ranks: List[pd.Series] = []
        for col in cols_used:
            if col in df.columns:
                # pct=True で 0〜1 にスケール、欠損はそのまま
                ranks.append(df[col].rank(pct=True, na_option="keep"))
        if ranks:
            df["growth_funda_v2"] = sum(ranks) / len(ranks)
```

#### 3.2 FACTORS_SCORE に追加

```python
FACTORS_SCORE = ["funda_score", "tech_score", "total_score", "growth_funda_v2"]
```

#### 3.3 `run_diagnosis` 内で IC 計算前に呼び出す

`run_diagnosis` の `ic_df = calc_rank_ic(snapshot_results)` の**直前**に：

```python
_cb("growth_funda_v2 計算中...")
_add_growth_funda_v2(snapshot_results)
```

### タスク 4: ファンダ・バリアント × 重みスイープ

**ファイル**: `services/diagnose_growth_service.py`

新規関数 `sweep_funda_variants` を追加（既存 `sweep_weights` のすぐ下に配置）：

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
    """
    funda スコアのバリアント × Funda/Tech 重み × Top N で Top N α を網羅計算する。

    Phase 2A 再設計のための候補配置の網羅評価が目的。

    Parameters
    ----------
    variants : 使用する funda スコア列のリスト（snapshot_results[i]["scored"] に存在する列名）
              デフォルト: ["funda_score", "growth_funda_v2"]
    weights  : (funda_w, tech_w) の組
              デフォルト: [(0.0, 1.0), (0.3, 0.7), (0.5, 0.5)]
    top_ns   : デフォルト [3, 5, 10]

    Returns
    -------
    DataFrame 列: variant / funda_w / tech_w / top_n / n / mean_return /
                  win_rate / topix_mean / alpha
    """
    if variants is None:
        variants = ["funda_score", "growth_funda_v2"]
    if weights is None:
        weights = [(0.0, 1.0), (0.3, 0.7), (0.5, 0.5)]
    if top_ns is None:
        top_ns = [3, 5, 10]

    bench_map = _precompute_bench_returns(
        snapshot_results, prices_df, forward_days, benchmark_code
    )

    rows = []
    for variant in variants:
        for fw, tw in weights:
            for top_n in top_ns:
                top_rets:   List[float] = []
                bench_rets: List[float] = []
                for r in snapshot_results:
                    if "scored" not in r:
                        continue
                    df = r["scored"].copy()
                    if variant not in df.columns:
                        continue
                    df["_total_w"] = (
                        df[variant].fillna(0)      * fw
                      + df["tech_score"].fillna(0) * tw
                    )
                    valid = df[df["has_fwd_data"]].copy()
                    top_valid = (valid.sort_values("_total_w", ascending=False)
                                      .head(top_n)["return_pct"].dropna())
                    if top_valid.empty:
                        continue
                    top_rets.extend(top_valid.tolist())
                    bret = bench_map.get(r["as_of"], np.nan)
                    if not np.isnan(bret):
                        bench_rets.extend([bret] * len(top_valid))

                if not top_rets:
                    rows.append({"variant": variant, "funda_w": fw, "tech_w": tw,
                                 "top_n": top_n, "n": 0,
                                 "mean_return": None, "win_rate": None,
                                 "topix_mean": None, "alpha": None})
                    continue

                s          = pd.Series(top_rets)
                bench_mean = float(np.nanmean(bench_rets)) if bench_rets else np.nan
                mean_ret   = round(float(s.mean()), 2)
                rows.append({
                    "variant":     variant,
                    "funda_w":     fw,
                    "tech_w":      tw,
                    "top_n":       top_n,
                    "n":           int(len(s)),
                    "mean_return": mean_ret,
                    "win_rate":    round(float((s > 0).sum()) / len(s) * 100, 1),
                    "topix_mean":  round(bench_mean, 2) if not np.isnan(bench_mean) else None,
                    "alpha":       round(mean_ret - bench_mean, 2)
                                   if not np.isnan(bench_mean) else None,
                })

    return (pd.DataFrame(rows)
              .sort_values("alpha", ascending=False, na_position="last")
              .reset_index(drop=True))
```

### タスク 5: `run_diagnosis` の戻り値・CSV に追加

**ファイル**: `services/diagnose_growth_service.py` の `run_diagnosis`

#### 5.1 sweep_funda_variants を呼び出す

`sweep_df = sweep_weights(...)` の**直後**に追加：

```python
_cb("ファンダ・バリアント × 重みスイープ計算中...")
variant_sweep_df = sweep_funda_variants(
    snapshot_results, prices_df, forward_days,
)
```

#### 5.2 CSV 保存に追加

`sweep_df.to_csv(...)` の直後に追加：

```python
variant_sweep_df.to_csv(
    os.path.join(_OUT_DIR, "funda_variant_sweep.csv"),
    index=False, encoding="utf-8-sig",
)
```

#### 5.3 戻り値辞書に追加

```python
return {
    ...,
    "weight_sweep":      sweep_df,
    "variant_sweep":     variant_sweep_df,   # ← 追加
    ...,
}
```

### タスク 6: CLI 出力に新セクションを追加

**ファイル**: `diagnose_growth.py` の `print_summary`

#### 6.1 ファクター IC 表示は変更不要

`growth_funda_v2` は `FACTORS_SCORE` に追加されたので自動的に IC 表に出る。

#### 6.2 重みスイープ表示の後に新セクションを追加

`# 重みスイープ` のループ表示の**後**に、以下を追加：

```python
# ファンダ・バリアント × 重みスイープ
variant_df = result.get("variant_sweep")
if variant_df is not None and not variant_df.empty:
    print(f"\n{'-'*W}")
    print(f"  [Funda variant × 重み × Top N スイープ]  alpha 降順 上位15件")
    print(f"{'-'*W}")
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

#### 6.3 CSV 保存先リストにも追加

```python
print(f"    ic_by_factor.csv  alpha_by_topn.csv  weight_sweep.csv  funda_variant_sweep.csv")
```

---

## 完了条件

1. `.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 20 --snapshots 1` がエラーなく完走する。
2. `.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 20`（全スナップショット）の出力に以下が含まれる：
   - Rank IC 表に `growth_funda_v2` 行がある
   - Top N α 表に top_n=3, 7, 15, 30 が増えている
   - 重みスイープに (0.0, 1.0), (0.2, 0.8), (0.3, 0.7), (0.4, 0.6) の行がある
   - 新セクション「Funda variant × 重み × Top N スイープ」が出力される
3. `data/diagnose_growth/funda_variant_sweep.csv` が生成される。
4. `data/diagnose_growth/ic_by_factor.csv` に `growth_funda_v2` 行がある。

---

## 実装上の注意

- **既存関数のシグネチャは変更しない**（呼び出し側の互換性を保つため）。デフォルト値だけを変更する。
- **新規ファイルは作らない**。すべて `services/diagnose_growth_service.py` と `diagnose_growth.py` への追記/修正で完結する。
- **cp932 文字を使わない**（罫線は `-` `=`、矢印は `<<` `<-`、ギリシャ文字は使わない、`α` は `alpha` と書く）。Windows コンソールが cp932 で動いているため。
- **`growth_funda_v2` の percentile rank** は **同一スナップショット内**で計算する（クロスセクション相対ランク）。`_add_growth_funda_v2` の実装はその通りになっている。
- ベンチマーク（TOPIX ETF code=`13060`）の forward リターンは `_precompute_bench_returns` 経由で再利用する（再計算しない）。

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

1. `growth_funda_v2` の Rank IC 値（mean_ic, t_stat）
2. 拡張した Top N α 表（top_n=3, 5, 7, 10, 15）の alpha 値
3. 重みスイープで tech-heavy 側 (0.0/1.0, 0.2/0.8, 0.3/0.7) の alpha 値
4. `funda_variant_sweep.csv` の上位 5 行（最も alpha が高い設定）

これらを基に Phase 2A の再設計仕様（ハードフィルタ・スコア・top_n・重み）を確定する。
