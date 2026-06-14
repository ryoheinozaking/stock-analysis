# 経営変化ボーナスの分解診断（IC + leave-one-out α）設計

- 作成日: 2026-06-14
- 対象: `services/pipeline_service.py`（calc_funda_score value分岐：ボーナス成分の列露出）+ `services/diagnose_value_service.py`（IC分解 / leave-one-out α）
- ステータス: 承認済み（設計）→ 実装計画へ
- 前提: [2026-06-14 Spearman タイ対応修正](../../../services/stats_utils.py)（#9）完了済み。離散・boolean を正しく測れる物差しが整っている。

## 背景・動機

バリュー funda_score の Rank IC は **+0.179**（fwd250）。ボーナス抜きコア `value_funda_v2` は **+0.146**、生PBR `|IC|` も **0.146**。

→ **value_funda_v2(0.146) ≈ 生PBR(0.146)** であり、PSR/PER/op_margin は PBR に上乗せをほぼ生まない。**生PBR を超えている唯一の要素が経営変化ボーナス（0.146 → 0.179、+0.033）**。

ところがボーナスは activist + V字転換 + 増益 + 増配 + 配当性向 の**未分解の塊**（最大 +55pt）。α 上乗せ分が全部ここに集約しているのに、どの成分が効いているか測っていない。本診断は各成分の寄与を分解し、効かない成分を落として効く成分に再配分する判断材料を作る。**測定のみ**。配点変更は数字を見てからの別ステップ。

## スコープと不変条件

- **診断のみ。`funda_score` の値・Top20 選定・α・ハードフィルタは一切変えない。**
- 触る箇所:
  1. `pipeline_service.calc_funda_score`（value 分岐）— 各ボーナスを合算前に列として保存。`score` への加算ロジックは不変 → **funda_score の値は不変**（ライブ挙動も不変）。
  2. `diagnose_value_service` — IC 分解の factor 追加、leave-one-out α の追加、専用 CSV 出力。
- 配点再配分は YAGNI（本設計に含めない）。

## activist の look-ahead 扱い

`data/governance_activists.json` は**現在スナップショット**で、過去時点の保有状況ではない。`calc_governance_score_for_df` は as_of に関係なく現在の json を読むため、過去スナップショットの activist 列は look-ahead バイアスを含む。

→ **activist は測るが「look-ahead・上限値」と明記**。IC 表・leave-one-out 表の activist 行にフラグを付け、**意思決定は point-in-time に再現可能な信号（div_trend / op_trend / op_turnaround / payout）だけで行う**。point-in-time activist の構築（過去の大株主履歴）は別プロジェクト級のため対象外。

## 設計

### 1. pipeline_service.calc_funda_score（value 分岐）— ボーナス成分の列露出

現状は各ボーナスを `score` に直接加算している。これを**一旦列に保存してから加算**する形に変える（値は不変）:

```python
# アクティビスト
gov = calc_governance_score_for_df(df, code_4_col="code_4")  # 0 / 10
df["activist"]        = gov > 0
df["bonus_activist"]  = gov
# 増配
df["bonus_div"]       = df["div_trend"].fillna(0).clip(0, 2) * 5.0
# 増益
df["bonus_op"]        = df["op_trend"].fillna(0).clip(0, 2) * 5.0
# V字転換
df["bonus_turnaround"] = df["op_turnaround"].eq(True).astype(float) * 15.0
# 配当性向（段階）
pr = df["payout_ratio"].fillna(-1)
df["bonus_payout"]    = (((pr >= 40) & (pr <= 70)).astype(float) * 10.0
                        + ((pr >= 25) & (pr < 40)).astype(float) * 5.0)
df["payout_in_band"]  = ((pr >= 25) & (pr <= 70)).astype(float)
df["bonus_total"]     = (df["bonus_activist"] + df["bonus_div"] + df["bonus_op"]
                        + df["bonus_turnaround"] + df["bonus_payout"])
score = score + df["bonus_total"]
```

`funda_score = score`（コア + bonus_total）は現状と同値。

**ガード**: `bonus_activist`/`activist` は `if "code_4" in df.columns` 分岐内で計算されるため、分岐に入る前に全ボーナス列を 0 で初期化し、`bonus_total` の合算が常に成立するようにする（live では code_4 常在だが防御的に）。同様に `div_trend`/`op_trend`/`op_turnaround`/`payout_ratio` 列が無い場合も 0 扱い（現状の `if ... in df.columns` を踏襲）。

### 2. IC 分解（diagnose_value_service.calc_rank_ic）

`scored` に上記列が乗るので、factor リストに追加するだけ:

```python
FACTORS_BONUS = [
    "div_trend", "op_trend", "op_turnaround",
    "payout_ratio", "payout_in_band",
    "bonus_total",
    "activist",          # look-ahead（出力で明記）
]
```

`div_trend`(0/1/2) と `bonus_div`(0/5/10) は単調変換で Spearman IC 同一のため、raw 信号で代表。payout は非単調なので連続値とバンド指標の両方。fwd250 と fwd60 の両方で測る。

### 3. leave-one-out α（diagnose_value_service）

各スナップショットの `scored` に「あるボーナスを抜いた funda」列を作る（`_add_value_funda_v2` と同じ後付けパターン）:

```python
def _add_bonus_variants(snapshot_results):
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        if "bonus_total" not in df.columns:
            continue
        f = df["funda_score"]
        df["funda_no_activist"]   = f - df["bonus_activist"]
        df["funda_no_div"]        = f - df["bonus_div"]
        df["funda_no_op"]         = f - df["bonus_op"]
        df["funda_no_turnaround"] = f - df["bonus_turnaround"]
        df["funda_no_payout"]     = f - df["bonus_payout"]
        df["funda_no_bonus"]      = f - df["bonus_total"]
```

`sweep_funda_variants` に variant 群を渡し、**本番設定 funda_w=0.6 / tech_w=0.4 × Top20** で α を計算:

```python
LOO_VARIANTS = ["funda_score", "funda_no_activist", "funda_no_div",
                "funda_no_op", "funda_no_turnaround", "funda_no_payout",
                "funda_no_bonus"]
loo_df = sweep_funda_variants(
    snapshot_results, prices_df, forward_days,
    variants=LOO_VARIANTS, weights=[(0.6, 0.4)], top_ns=[20],
)
# 各 funda_no_X の alpha と funda_score の alpha の差 = X の限界寄与
```

`funda_score`（フル）の α を基準に各 `funda_no_X` の α 低下分を「X の限界α寄与」として算出（差分列を付与）。

### 4. 出力 CSV（衝突回避のため専用名）

- `data/diagnose_value/bonus_ic.csv` — ボーナス成分の単独 IC（mean/median/pos%/t、activist に look_ahead フラグ列）
- `data/diagnose_value/bonus_leave_one_out.csv` — 各 variant の Top20 α と、フルとの差分（`alpha_drop_vs_full`、look_ahead フラグ）

既知挙動: 診断スクリプトは fwd で名前を分けず `ic_by_factor.csv` 等の無印を上書きする。ボーナス系は専用名で出すので衝突しない。`run_diagnosis` の戻り辞書に `bonus_ic` / `bonus_leave_one_out` を追加し、`diagnose_value.py` の保存・サマリー表示に組み込む。

## テスト（TDD）

`tests/test_bonus_decomposition.py`（新規、合成 DataFrame）:

- **不変条件**: ボーナス成分列を入れても `calc_funda_score(mode="value")` の `funda_score` が従来値と一致（成分を持つ行・持たない行の両方）。
- `bonus_total` == `bonus_activist + bonus_div + bonus_op + bonus_turnaround + bonus_payout`。
- 各 `bonus_*` の点数が定義どおり（div_trend=2→10、op_turnaround=True→15、payout 50→10、payout 30→5、payout 80→0、payout_in_band の境界）。
- variant 再構成: `funda_no_X == funda_score - bonus_X`（合成 scored で）。
- 既存 38 件が緑のまま。

leave-one-out / IC 集計の数値そのもの（実データ依存）はテスト対象外。純粋な算術・列生成ロジックをテストする。

## 受け入れ基準

1. `calc_funda_score(mode="value")` が `bonus_activist/div/op/turnaround/payout`・`bonus_total`・`payout_in_band`・`activist` 列を出し、`funda_score` 値は不変。
2. `diagnose_value.py` 実行で `bonus_ic.csv` と `bonus_leave_one_out.csv` が生成される。
3. activist 行に look-ahead フラグが付く。
4. ユニットテストが通り、既存テストが壊れていない。
5. growth モードの挙動・funda_score 値・Top20 選定・既存 α 系 CSV に影響がない。

## スコープ外（数字を見てからの別ステップ）

- ボーナス配点の再配分・撤廃（効かない成分を落とす）。
- 増分IC（PBR 残差化後の各ボーナスの直交寄与）。標準のleave-one-out αで代替。
- point-in-time activist データの構築。
