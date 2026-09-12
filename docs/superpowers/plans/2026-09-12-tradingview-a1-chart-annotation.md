# TradingView A-1（分析結果のチャート描き込み）実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 深層分析・飛躍分析が算出したエントリー／利確／損切り水準を、TradingView のチャートに線とアラートとして描き込む。

**Architecture:** 専用スクリプトは作らず、2 つのスキル（`deep-analysis-jp` / `leap-analysis-jp`）の `SKILL.md` に手順を追加する。書き込みは MCP ツールを直接呼び、読み取りだけは暗号化ブロブ回避のため CLI とパイプを経由する。描画 ID はチャート文脈ごとに JSON へ記録し、再分析時は二段階更新（作成→検証→旧削除）で置き換える。

**Tech Stack:** TradingView MCP (84 tools) / Chrome DevTools Protocol / Node.js CLI / Markdown（スキル定義）

---

## 設計書

`docs/superpowers/specs/2026-09-12-tradingview-integration-design.md` の
「A-1 設計（2026-09-12 確定）」節

## 前提状態

- TradingView MCP は `claude mcp add -s user` で登録済み、固定 SHA `c05b8f5755ed8e64ea242de88ddbf46aa24d56a4`
- `package-lock.json` sha256: `92c8603fa00c1556d6f806cb9a06329df9d10985559da9057af40ed01d09cf8d`
- `data/tradingview/` は `.gitignore` 済み
- 現在のブランチ: `docs/tradingview-a1`
- A-1 の書き込み系ツールは実機検証済み（draw_shape / alert_create / 個別削除 / 復元）

## 変更対象ファイル

| ファイル | 責務 | git |
|---|---|---|
| `~/.claude/skills/deep-analysis-jp/SKILL.md` | Step 1.6 追加、4.5 を 2 層構成に | **管理外** |
| `~/.claude/skills/leap-analysis-jp/SKILL.md` | Step 1.6 追加、「4. タイミングと価格」を拡張 | **管理外** |
| `data/tradingview/drawings/` | 描画 ID の記録置き場（新規ディレクトリ） | 除外済み |
| `data/tradingview/backup/` | SKILL.md の退避先 | 除外済み |

**両 SKILL.md は git 管理外でバックアップが存在しない。** Task 1 の退避を飛ばしてはならない。

## 禁止事項（実装中に絶対に守る）

- `draw_clear` を呼ばない（利用者の手描き描画を巻き込んで消す）
- `alert_delete` に `delete_all: true` を渡さない（既存アラートを消す）
- 削除は必ず ID 指定で行う
- 旧描画を削除してから新規作成しない（順序を逆にする。Task 5 参照）

---

### Task 1: バックアップとディレクトリ準備

**Files:**
- Create: `data/tradingview/drawings/`
- Create: `data/tradingview/backup/SKILL_deep-analysis-jp_2026-09-12.md`
- Create: `data/tradingview/backup/SKILL_leap-analysis-jp_2026-09-12.md`

- [ ] **Step 1: 記録用ディレクトリを作成する**

```bash
mkdir -p ~/stock_analysis/data/tradingview/drawings
```

- [ ] **Step 2: 両 SKILL.md を退避する**

```bash
cp ~/.claude/skills/deep-analysis-jp/SKILL.md ~/stock_analysis/data/tradingview/backup/SKILL_deep-analysis-jp_2026-09-12.md
cp ~/.claude/skills/leap-analysis-jp/SKILL.md ~/stock_analysis/data/tradingview/backup/SKILL_leap-analysis-jp_2026-09-12.md
```

- [ ] **Step 3: 退避が正しく行われたか行数で確認する**

```bash
wc -l ~/.claude/skills/deep-analysis-jp/SKILL.md ~/stock_analysis/data/tradingview/backup/SKILL_deep-analysis-jp_2026-09-12.md ~/.claude/skills/leap-analysis-jp/SKILL.md ~/stock_analysis/data/tradingview/backup/SKILL_leap-analysis-jp_2026-09-12.md
```

Expected: deep-analysis-jp が 605 行で 2 つとも一致、leap-analysis-jp が 327 行で 2 つとも一致

行数が一致しない場合は退避に失敗しているため、**ここで停止して原因を調べる**。

- [ ] **Step 4: 退避ファイルが git に載らないことを確認する**

```bash
cd ~/stock_analysis && git status --short
```

Expected: `data/tradingview/` 配下のファイルが出力に現れない

---

### Task 2: deep-analysis-jp に Step 1.6 を追加

**Files:**
- Modify: `~/.claude/skills/deep-analysis-jp/SKILL.md`（`### Step 2: フレームワーク適用` の直前）

- [ ] **Step 1: 挿入位置を確認する**

```bash
grep -n "^### Step 2: フレームワーク適用" ~/.claude/skills/deep-analysis-jp/SKILL.md
```

Expected: `224:### Step 2: フレームワーク適用`

- [ ] **Step 2: Step 1.6 の節を挿入する**

`### Step 2: フレームワーク適用` の直前に、以下をそのまま挿入する。

````markdown
### Step 1.6: TradingView チャート連携（株価が論点のときのみ・任意）

**実行条件**: 株価水準・エントリータイミング・利確損切りが論点になっている場合のみ実行する。
事業構造や財務が中心の分析ではスキップする。

**位置づけ**: 規約適合を保証しない実験的なローカル支援。発注・自動売買には一切接続しない。
背景と判断根拠は `C:\Users\ryohei\stock_analysis\docs\superpowers\specs\2026-09-12-tradingview-integration-design.md` を参照。

**絶対に守ること**:
- `draw_clear` を呼ばない（利用者の手描き描画を巻き込んで消す）
- `alert_delete` に `delete_all: true` を渡さない
- 削除は必ず ID 指定で行う
- **失敗しても分析は止めない**。警告を出して通常の分析を完遂する

#### 1.6.1 接続とチャート文脈の記録

1. `tv_health_check` を呼ぶ
2. `cdp_connected: false` またはエラーの場合:
   - TradingView が起動していなければ `tv_launch`（`port: 9222`）を実行
   - 起動中なら「CDP を有効にするため TradingView を再起動します」と伝えてから `tv_launch` を実行
   - それでも失敗したらこの節を丸ごとスキップし、通常の分析を続行する
3. `chart_get_state` の `symbol` と `resolution` を控える（復元用）
4. `layout_list` を呼び、`modified` が最新のレイアウトの `id` を控える

#### 1.6.2 分析対象への切り替えと指標読み取り

1. `chart_set_symbol` で分析対象へ切り替える（例: `TSE:7011`）
   - エラーになる場合は `symbol_search`（`query` に 4 桁コード）で正式シンボルを引く
2. 指標値は **CLI 経由で取得する**。MCP の `data_get_study_values` は
   `inputs` に暗号化ブロブを含み、1 回でコンテキストを大量消費するため直接呼ばない:

```bash
cd C:\Users\ryohei\tradingview-mcp && node src/cli/index.js values | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const o=JSON.parse(s);console.log(JSON.stringify((o.studies||[]).map(t=>({name:t.name,values:t.values})),null,1))})"
```

3. 取得した値（Supertrend・一目均衡表・SAR・BB 等）は 4.5 で J-Quants の数値と並べて記載する

#### 1.6.3 水準の描き込み（分析で水準が確定した後に実行）

記録ファイル: `C:\Users\ryohei\stock_analysis\data\tradingview\drawings\<4桁コード>.json`

**手順は「作ってから消す」。旧を先に消してはならない**（作成が途中で失敗すると前回の水準を失うため）。

1. 記録ファイルがあれば読み、`layout_id` と `tv_symbol` が**両方一致**する context を探す
2. 新しい線を `draw_shape` で引く（旧線は残したまま）。`point.time` は直近バーの unix 秒:

| 水準 | shape | overrides | text |
|---|---|---|---|
| エントリー | `horizontal_line` | `{"linecolor": "#00c853", "linewidth": 2}` | `[DA] エントリー 3,200` |
| 第 1 利確 | `horizontal_line` | `{"linecolor": "#2962ff", "linewidth": 2}` | `[DA] 利確1 4,300` |
| 第 2 利確 | `horizontal_line` | `{"linecolor": "#2962ff", "linewidth": 2}` | `[DA] 利確2 4,500` |
| 損切り | `horizontal_line` | `{"linecolor": "#ff5252", "linewidth": 2}` | `[DA] 損切り 2,900` |

3. `alert_create` でエントリーと損切りにアラートを作る（利確には作らない）
   - エントリー: `condition: "less_than"`、`price` はエントリー水準
   - 損切り: `condition: "less_than"`、`price` は損切り水準
   - `message` は `[DA] <銘柄コード> <役割> <価格>` 形式
4. `draw_list` と `alert_list` を呼び、作成した ID がすべて存在することを確認する
5. 確認できたら記録ファイルを書く（下記の構造）
6. **記録を書き終えてから**、旧 context の `shapes[].id` を `draw_remove_one` で、
   `alerts[].id` を `alert_delete`（`alert_id` 指定）で個別に削除する
7. 4 の確認で 1 件でも欠けていた場合は、**作成済みの新規分だけを ID 指定で削除**し、
   旧 context はそのまま残す。そのうえで「描き込みに失敗したため前回の水準を維持した」と報告する

記録ファイルの構造:

```json
{
  "ticker": "7011",
  "contexts": [
    {
      "layout_id": 65538569,
      "tv_symbol": "TSE_DLY:7011",
      "resolution": "1D",
      "updated_at": "2026-09-12",
      "analysis_type": "deep-analysis-jp",
      "shapes": [
        { "id": "jQbi4o", "role": "entry", "price": 3200, "label": "[DA] エントリー 3,200" }
      ],
      "alerts": [
        { "id": 5593961826, "role": "entry", "price": 3200 }
      ]
    }
  ]
}
```

- `layout_id` と `tv_symbol` が両方一致する context のみ削除対象とする
- 一致する context がなければ削除せず、新しい context として配列に追記する
- ID が無効（利用者が手で消した等）でエラーになっても無視して続行する

#### 1.6.4 チャート文脈の復元

`chart_set_symbol` で 1.6.1 で控えた元の銘柄へ戻す。
`resolution` が変わっていれば `chart_set_timeframe` で戻す。
````

- [ ] **Step 3: 挿入後の行数を確認する**

```bash
wc -l ~/.claude/skills/deep-analysis-jp/SKILL.md
```

Expected: 605 行から増えている（おおよそ 700 行前後）

- [ ] **Step 4: 挿入位置が正しいか見出しの並びで確認する**

```bash
grep -nE "^### Step 1\.5|^### Step 1\.6|^### Step 2" ~/.claude/skills/deep-analysis-jp/SKILL.md
```

Expected: Step 1.5 → Step 1.6 → Step 2 の順で並ぶ

---

### Task 3: deep-analysis-jp の 4.5 を 2 層構成にする

**Files:**
- Modify: `~/.claude/skills/deep-analysis-jp/SKILL.md`（`### 4.5 テクニカル状況` の節）

- [ ] **Step 1: 現在の 4.5 の末尾を確認する**

```bash
grep -n "WebFetch の数値（kabutan・Yahoo）と J-Quants の数値が食い違う場合" ~/.claude/skills/deep-analysis-jp/SKILL.md
```

Expected: 1 行ヒットする（4.5 節の最終段落）

- [ ] **Step 2: その段落の直後に 2 層構成の指示を追加する**

`WebFetch の数値（kabutan・Yahoo）と J-Quants の数値が食い違う場合は**両方明記**し、出典を明示。`
の直後に、以下を挿入する。

````markdown
**Step 1.6 を実行した場合は 2 層で記載する**:

1. **J-Quants 客観指標** — 全銘柄同一基準で、バックテストで検証済みの数値
2. **利用者のチャート環境** — Supertrend・一目均衡表・SAR・BB 等、実際に判断に使っている道具

両者が食い違ったとき（例: J-Quants の RSI は中立だが Supertrend は売り継続）は、
どちらが正しいかを断定せず**食い違いそのものを論点として提示する**。
なお TradingView 側は 20 分遅延（`TSE_DLY`）、J-Quants 側は parquet の更新日に依存するため、
**両者の as_of が異なる場合は必ず明示する**（日付のずれを指標の差と誤認しないため）。

描き込んだ水準（エントリー／利確／損切り）は、レポートにも同じ数値を記載して照合できるようにする。
````

- [ ] **Step 3: 変更が入ったか確認する**

```bash
grep -n "Step 1.6 を実行した場合は 2 層で記載する" ~/.claude/skills/deep-analysis-jp/SKILL.md
```

Expected: 1 行ヒットする

---

### Task 4: leap-analysis-jp に同じ連携を追加

**Files:**
- Modify: `~/.claude/skills/leap-analysis-jp/SKILL.md`（`### Step 2: フレームワーク適用` の直前、および `## 4. タイミングと価格` の節）

- [ ] **Step 1: 挿入位置を確認する**

```bash
grep -nE "^### Step 2: フレームワーク適用|^## 4\. タイミングと価格" ~/.claude/skills/leap-analysis-jp/SKILL.md
```

Expected: `166:### Step 2: フレームワーク適用` と `208:## 4. タイミングと価格`

- [ ] **Step 2: Step 1.6 を挿入する**

`### Step 2: フレームワーク適用` の直前に、**Task 2 の Step 2 で挿入した節の全文**を挿入する。

コピーする範囲は、Task 2 の Step 2 に記載された
`### Step 1.6: TradingView チャート連携（株価が論点のときのみ・任意）` の行から、
`#### 1.6.4 チャート文脈の復元` 節の末尾（`resolution` が変わっていれば〜戻す。）までの全体。

コピー後、**1 箇所だけ書き換える**。

```
変更前: "analysis_type": "deep-analysis-jp",
変更後: "analysis_type": "leap-analysis-jp",
```

他の記述（ツール名・色コード・パス・手順）はすべて同一のまま使う。

- [ ] **Step 3: 「4. タイミングと価格」に描き込みの指示を追加する**

`## 4. タイミングと価格` の節の末尾に、以下を追加する。

````markdown
**Step 1.6 を実行した場合**: SEPA ステージ判定と PEG による妥当水準から導いた
エントリー／利確／損切りを、Step 1.6.3 の手順でチャートに描き込む。
レポートにも同じ数値を記載し、チャートと照合できるようにする。

TradingView の Supertrend・一目均衡表の水準と、自分が導いた水準が食い違う場合は、
**食い違いを論点として明示する**（どちらかを正解として断定しない）。
````

- [ ] **Step 4: 両方の変更が入ったか確認する**

```bash
grep -nE "^### Step 1\.6|Step 1.6 を実行した場合" ~/.claude/skills/leap-analysis-jp/SKILL.md
```

Expected: Step 1.6 の見出しと、「4. タイミングと価格」への追記の 2 箇所がヒットする

- [ ] **Step 5: leap 側の analysis_type が正しいか確認する**

```bash
grep -n "leap-analysis-jp\"" ~/.claude/skills/leap-analysis-jp/SKILL.md
```

Expected: `"analysis_type": "leap-analysis-jp"` がヒットする（deep-analysis-jp が残っていない）

---

### Task 5: 連携の単体検証（初回描画）

深層分析を丸ごと走らせると時間とトークンを大量に使うため、**連携部分だけを手順どおり実行して検証する**。
対象は 5016（JX 金属、現在チャートに表示中）。水準は検証用の仮値を使う。

- [ ] **Step 1: CDP 接続を確立する**

`tv_health_check` を呼ぶ。`cdp_connected: false` なら `tv_launch`（`port: 9222`）を実行してから再度呼ぶ。

Expected: `cdp_connected: true`、`chart_symbol: "TSE_DLY:5016"`

- [ ] **Step 2: チャート文脈を記録する**

`chart_get_state` と `layout_list` を呼び、`symbol` / `resolution` / 最新の `layout_id` を控える。

Expected: symbol `TSE_DLY:5016`、resolution `1D`、layout_id `65538569`

- [ ] **Step 3: 既存の描画とアラートを記録する（復元確認用）**

`draw_list` と `alert_list` を呼び、件数と ID を控える。

Expected: 描画 3 件（`53kbld` / `DuNpvv` / `r8g3K4`）、アラート 1 件（`4806548286`）

- [ ] **Step 4: 4 本の線を引く**

以下の仮水準で `draw_shape` を 4 回呼ぶ。`point.time` は `1789084800`（直近バー）。

| 役割 | price | linecolor | text |
|---|---|---|---|
| entry | 3400 | `#00c853` | `[DA] エントリー 3,400` |
| tp1 | 4300 | `#2962ff` | `[DA] 利確1 4,300` |
| tp2 | 4600 | `#2962ff` | `[DA] 利確2 4,600` |
| stop | 3100 | `#ff5252` | `[DA] 損切り 3,100` |

Expected: 4 回とも `success: true` で `entity_id` が返る

- [ ] **Step 5: アラートを 2 件作る**

`alert_create` を 2 回呼ぶ。

- entry: `condition: "less_than"`, `price: 3400`, `message: "[DA] 5016 エントリー 3400"`
- stop: `condition: "less_than"`, `price: 3100`, `message: "[DA] 5016 損切り 3100"`

Expected: 2 回とも `success: true` で `alert_id` が返る

- [ ] **Step 6: 作成結果を検証する**

`draw_list` と `alert_list` を呼ぶ。

Expected: 描画 7 件（既存 3 + 新規 4）、アラート 3 件（既存 1 + 新規 2）。
Step 4・5 で返った ID がすべて含まれること

- [ ] **Step 7: 記録ファイルを書く**

`data/tradingview/drawings/5016.json` に、Task 2 で定義した構造で書き出す。
`analysis_type` は `"a0-a1-verification"` とする（本番の分析ではないため）。

- [ ] **Step 8: 画面を目視確認する**

`capture_screenshot`（`region: "chart"`, `wait_for_render: true`）を撮り、Read で開く。

Expected: 緑 1 本・青 2 本・赤 1 本の水平線とラベルが表示され、
既存の手描き描画（白い斜線）も残っていること

---

### Task 6: 二段階更新の検証（再実行）

Task 5 の状態から、水準を変えて再実行し、旧線が正しく置き換わるかを確認する。
**これが A-1 の中核ロジックの検証**である。

- [ ] **Step 1: 記録ファイルを読む**

`data/tradingview/drawings/5016.json` を Read し、`layout_id` と `tv_symbol` が
現在のチャート文脈と一致することを確認する。

Expected: `layout_id: 65538569`、`tv_symbol: "TSE_DLY:5016"` が一致

- [ ] **Step 2: 新しい水準で線を引く（旧線は残したまま）**

`draw_shape` を 4 回呼ぶ。今回は水準を変える。

| 役割 | price | linecolor | text |
|---|---|---|---|
| entry | 3500 | `#00c853` | `[DA] エントリー 3,500` |
| tp1 | 4400 | `#2962ff` | `[DA] 利確1 4,400` |
| tp2 | 4700 | `#2962ff` | `[DA] 利確2 4,700` |
| stop | 3200 | `#ff5252` | `[DA] 損切り 3,200` |

Expected: 4 回とも成功。この時点で描画は 11 件（既存 3 + 旧 4 + 新 4）

- [ ] **Step 3: 新しいアラートを作る**

`alert_create` を 2 回呼ぶ（entry 3500 / stop 3200、いずれも `less_than`）。

Expected: 2 件とも成功。アラートは 5 件（既存 1 + 旧 2 + 新 2）

- [ ] **Step 4: 作成を検証する**

`draw_list` と `alert_list` で、新規 ID がすべて存在することを確認する。

Expected: 描画 11 件、アラート 5 件

- [ ] **Step 5: 記録ファイルを新 ID で更新する**

`data/tradingview/drawings/5016.json` の該当 context を新 ID に書き換える。
**このとき旧 ID を別途控えておく**（次のステップで削除するため）。

- [ ] **Step 6: 旧 ID だけを削除する**

控えた旧 ID に対して `draw_remove_one` を 4 回、`alert_delete`（`alert_id` 指定）を 2 回呼ぶ。

Expected: すべて成功

- [ ] **Step 7: 最終状態を検証する**

`draw_list` と `alert_list` を呼ぶ。

Expected: **描画 7 件（既存 3 + 新 4）、アラート 3 件（既存 1 + 新 2）**。
既存の `53kbld` / `DuNpvv` / `r8g3K4` と `4806548286` が無傷で残っていること

- [ ] **Step 8: 画面を目視確認する**

`capture_screenshot` を撮って Read で開く。

Expected: 線が 3,500 / 4,400 / 4,700 / 3,200 の位置にあり、
Task 5 の水準（3,400 / 4,300 / 4,600 / 3,100）の線が消えていること

---

### Task 7: 後片付けと結果の記録

**Files:**
- Modify: `docs/superpowers/specs/2026-09-12-tradingview-integration-design.md`

- [ ] **Step 1: 検証で作った描画とアラートを全て削除する**

記録ファイルの ID に対して `draw_remove_one` を 4 回、`alert_delete` を 2 回呼ぶ。

**`draw_clear` と `delete_all` は使わない。**

- [ ] **Step 2: 元の状態に戻ったことを確認する**

`draw_list` と `alert_list` を呼ぶ。

Expected: 描画 3 件（`53kbld` / `DuNpvv` / `r8g3K4`）、アラート 1 件（`4806548286`）。
Task 5 Step 3 で控えた内容と完全一致

- [ ] **Step 3: 検証用の記録ファイルを削除する**

```bash
rm ~/stock_analysis/data/tradingview/drawings/5016.json
```

- [ ] **Step 4: CDP を閉じる**

```bash
taskkill //F //IM TradingView.exe
```

ポートが閉じたことを確認する。

```bash
netstat -ano | grep 9222 || echo "port 9222 closed"
```

Expected: `port 9222 closed`

- [ ] **Step 5: 設計書に A-1 実施結果を追記する**

設計書の「## 保留事項」の直前に、以下の構成で追記する。

```markdown
## A-1 実施結果（2026-09-12）

### 変更したファイル

- `~/.claude/skills/deep-analysis-jp/SKILL.md` — Step 1.6 を追加、4.5 を 2 層構成に
- `~/.claude/skills/leap-analysis-jp/SKILL.md` — Step 1.6 を追加、「4. タイミングと価格」を拡張
- 退避: `data/tradingview/backup/SKILL_*_2026-09-12.md`

### 検証結果

| 項目 | 結果 |
|---|---|
| 初回描画（4 本の線 + 2 件のアラート） | OK / NG |
| 二段階更新（新規作成 → 検証 → 旧削除） | OK / NG |
| 既存の手描き描画・アラートの保全 | OK / NG |
| 元の状態への復元 | OK / NG |

### 分かったこと・制約

（実際に試して判明した事項を記す）
```

実際の結果に置き換えて記入する。OK / NG はどちらか一方を残す。

- [ ] **Step 6: コミット**

```bash
cd ~/stock_analysis && git add docs/superpowers/specs/2026-09-12-tradingview-integration-design.md && git commit -m "docs: A-1実施結果を設計書に追記

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

- [ ] **Step 7: 利用者に報告する**

検証結果と、次に実際の深層分析で使えるようになったことを伝える。
TradingView が通常状態に戻っていること、CDP が閉じていることもあわせて報告する。

---

## 実装後に残ること

- **本番の深層分析での初回実行は未検証**。Task 5-6 は連携部分の単体検証であり、
  実際の分析フロー全体に組み込まれた状態では試していない。
  次に株価が論点の分析を行うとき、想定どおり動くかを確認する
- A-2（候補の一括巡回）で `watchlist_add_bulk` が使えるかは未検証（A-0 の申し送り）
