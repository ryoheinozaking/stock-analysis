# TradingView MCP A-0（技術検証＋設定退避）実装計画

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** TradingView MCP が MSIX 環境で動作するかを 8 項目の判定表で確認し、レイアウトとウォッチリストを退避する。

**Architecture:** MCP サーバーを固定コミットで登録し、Claude Code 経由でツールを呼んで実データを取得する。取得結果は `data/tradingview/` 配下（git 管理外）に保存し、判定結果は設計書に追記する。コードはほぼ書かず、検証手順が主体。

**Tech Stack:** Node.js 24 / `@modelcontextprotocol/sdk` / Chrome DevTools Protocol (port 9222) / TradingView Desktop (MSIX)

---

## 設計書

`docs/superpowers/specs/2026-09-12-tradingview-integration-design.md`

## 重要な制約: セッションが一度切れる

MCP サーバーは Claude Code の起動時にしか読み込まれない。
そのため **Task 2 完了後に Claude Code の再起動が必要**で、Task 3 以降は別セッションになる。
本計画書が引き継ぎ資料を兼ねる。再起動後は本ファイルを読んで Task 3 から再開すること。

## 前提状態（Task 1 開始時点）

- `~/tradingview-mcp` に clone 済み、`npm install` + `npm audit fix` 実行済み
- 固定対象コミット: `c05b8f5`
- `~/.claude/.mcp.json` に tradingview エントリが残っているが**この場所は読み込まれない**。Task 2 で削除する
- TradingView Desktop は MSIX 版がインストール済み（`TradingView.Desktop_n534cwy3pjxzj`）
- 現在のブランチ: `docs/tradingview-integration`

## 判定表（Task 10 で記入）

| # | 判定項目 | 成功基準 | 対応 Task |
|---|---|---|---|
| 1 | MSIX 環境での起動 | `tv_health_check` が `cdp_connected: true` | Task 3-4 |
| 2 | CDP バインド | `127.0.0.1` のみで待ち受け | Task 4 |
| 3 | 指標値の取得 | Supertrend / SQZMOM の現在値が数値で読める | Task 5 |
| 4 | Pine 描画の取得 | `data_get_pine_lines` / `labels` が意味のある値を返す | Task 6 |
| 5 | CLI の非対話出力 | `tv` がパイプ可能な JSON を返す | Task 7 |
| 6 | 起動済み TradingView の復旧 | `taskkill` 後もログイン状態・レイアウトが保持される | Task 4, 9 |
| 7 | CDP 終了後の復旧 | ポートを閉じた後、通常起動で問題なく使える | Task 9 |
| 8 | 再現性 | 固定 SHA `c05b8f5` で 1〜7 が再現する | Task 10 |

**撤退条件**: 1 または 6 が NG なら系統 A を中止し B-1 へ切り替える。
3 と 4 が両方 NG なら A-1 の価値が消えるため A-1 を中止する。

---

### Task 1: 作業ディレクトリと除外設定の準備

**Files:**
- Create: `data/tradingview/state/`（ディレクトリのみ。git 管理外のため `.gitkeep` は置かない）
- Create: `data/tradingview/backup/`
- Create: `data/tradingview/screenshots/`
- Modify: `.gitignore`

- [ ] **Step 1: ディレクトリを作成する**

```bash
mkdir -p ~/stock_analysis/data/tradingview/state ~/stock_analysis/data/tradingview/backup ~/stock_analysis/data/tradingview/screenshots
```

- [ ] **Step 2: `.gitignore` に除外設定を追記する**

`.gitignore` の `data/margin/` の行の直後に以下を追記する。

```
data/tradingview/
```

`data/*.json` はサブディレクトリ内のファイルにマッチしないため、この明示が必要。

- [ ] **Step 3: 除外が効いていることを確認する**

```bash
cd ~/stock_analysis && touch data/tradingview/state/probe.json && git status --short
```

Expected: `probe.json` が `git status` に現れない（出力に `data/tradingview` を含む行がない）

- [ ] **Step 4: 確認用ファイルを削除する**

```bash
rm ~/stock_analysis/data/tradingview/state/probe.json
```

- [ ] **Step 5: コミット**

```bash
cd ~/stock_analysis && git add .gitignore && git commit -m "chore: TradingView検証データ用のディレクトリを除外設定に追加

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

### Task 2: MCP サーバーの固定と登録

**Files:**
- Modify: `~/.claude/.mcp.json`（tradingview エントリを削除）
- Modify: `~/.claude.json`（`claude mcp add` が自動で書き込む）

- [ ] **Step 1: 固定コミットであることを確認する**

```bash
cd ~/tradingview-mcp && git rev-parse HEAD && git status --short
```

Expected: `c05b8f5755ed8e64ea242de88ddbf46aa24d56a4` が表示され、作業ツリーがクリーン（出力なし）

異なる SHA の場合は `git checkout c05b8f5` で固定してから進む。

- [ ] **Step 2: 効かない設定ファイルから tradingview エントリを削除する**

`~/.claude/.mcp.json` を編集し、`tradingview` エントリを削除して以下の状態に戻す。
このファイルは読み込まれないため、残すと将来の混乱の元になる。

```json
{
  "mcpServers": {
    "edinetdb": {
      "type": "http",
      "url": "https://mcp.edinetdb.jp/mcp",
      "headers": {
        "Authorization": "Bearer edb_ebb8065c25046dc5858f4b136564a2cb"
      }
    }
  }
}
```

- [ ] **Step 3: 正しい場所に登録する**

```bash
claude mcp add -s user tradingview -- node C:/Users/ryohei/tradingview-mcp/src/server.js
```

Expected: 登録成功のメッセージ

- [ ] **Step 4: 登録内容を確認する**

```bash
claude mcp list
```

Expected: `obsidian-vault` に加えて `tradingview` が一覧に現れる

- [ ] **Step 5: コミット不要・ユーザーに再起動を依頼する**

設定ファイルはリポジトリ外のため commit 対象なし。
ユーザーに以下を伝えて再起動してもらう。

> MCPサーバーを登録しました。反映にはClaude Codeの再起動が必要です。
> 再起動後、この計画書の Task 3 から再開します。

---

### Task 3: 接続確認

**再起動後のセッションはここから開始する。**

- [ ] **Step 1: ツールがロードされたか確認する**

ToolSearch を以下のクエリで実行する。

```
select:mcp__tradingview__tv_health_check,mcp__tradingview__tv_launch,mcp__tradingview__chart_get_state
```

Expected: 3 つのツール定義が返る

返らない場合はツール名の接頭辞が異なる可能性があるため、`ToolSearch` に `tradingview` をキーワード指定して一覧を確認する。

- [ ] **Step 2: 接続状態を確認する**

`tv_health_check` を引数なしで呼ぶ。

Expected（TradingView が CDP 有効で起動済みの場合）:
```json
{ "success": true, "cdp_connected": true, "chart_symbol": "...", "api_available": true }
```

`cdp_connected: false` または接続エラーの場合は Task 4 へ進む。
`cdp_connected: true` なら Task 4 の Step 1-2 を飛ばし Step 3 から実施する。

---

### Task 4: TradingView 起動と CDP バインド確認

- [ ] **Step 1: 起動前にユーザーへ確認する**

`tv_launch` は既定で既存の TradingView を `taskkill` してから起動し直す。
開いているチャートが一度閉じることをユーザーに伝え、了解を得てから実行する。

MSIX フォールバックが走る場合、約 330MB のローカルコピー作成に 1 分程度かかる。

- [ ] **Step 2: CDP 有効で起動する**

`tv_launch` を引数 `{ "port": 9222 }` で呼ぶ。

Expected: `success: true`。MSIX フォールバックが走った場合は `msix_local_copy: true` が含まれる

失敗した場合は同梱のバッチスクリプトを試す。PowerShell ツールで以下を実行する
（Git Bash からのバッチ呼び出しはエスケープが不安定なため PowerShell を使う）。

```powershell
Set-Location $env:USERPROFILE\tradingview-mcp; .\scripts\launch_tv_debug.bat 9222
```

Expected: `CDP ready at http://127.0.0.1:9222` と表示される

このスクリプトは最初に `taskkill /F /IM TradingView.exe` を実行し、`Get-AppxPackage` で
MSIX の実体を解決してから起動する。ただし WindowsApps からの直接起動が
"Access is denied" になる環境ではここで失敗する（その場合の自動フォールバックは
`tv_launch` 側にしかない）。

それでも駄目なら手動フォールバックを試す。

```powershell
$pkg = (Get-AppxPackage TradingView.Desktop).InstallLocation
New-Item -ItemType Directory -Force "$env:LOCALAPPDATA\tradingview-mcp\TradingView" | Out-Null
Copy-Item "$pkg\*" "$env:LOCALAPPDATA\tradingview-mcp\TradingView" -Recurse -Force
& "$env:LOCALAPPDATA\tradingview-mcp\TradingView\TradingView.exe" --remote-debugging-port=9222
```

`icacls` で WindowsApps の権限を変更してはならない（失敗する上にアプリの更新機構を壊す）。

これらを全て試して駄目なら**判定項目 1 を NG として Task 10 へ飛ぶ**。

- [ ] **Step 3: 接続を再確認する**

`tv_health_check` を呼ぶ。

Expected: `cdp_connected: true`

これが **判定項目 1** の結果になる。

- [ ] **Step 4: CDP のバインドアドレスを確認する**

```bash
netstat -ano | grep 9222
```

Expected: `127.0.0.1:9222` で LISTENING。`0.0.0.0:9222` や `[::]:9222` が現れないこと

これが **判定項目 2** の結果になる。`0.0.0.0` の場合は外部公開されているため、
ユーザーに報告して即座に TradingView を終了する。

- [ ] **Step 5: ログイン状態とレイアウトの保持を確認する**

`tv_health_check` の戻り値の `chart_symbol` を確認し、あわせて `capture_screenshot` を
引数 `{ "region": "full" }` で呼んで画面を取得する。返されたファイルパスを Read で開く。

Expected: ログイン済みで、再起動前と同じレイアウト（インジケーターが乗った状態）が表示されている

これが **判定項目 6** の前半の結果になる。

---

### Task 5: チャート状態と指標値の取得

- [ ] **Step 1: チャート状態を取得する**

`chart_get_state` を引数なしで呼ぶ。

Expected: `symbol`, `timeframe`, `chart_type`, インジケーター一覧（entity ID 付き）が返る

- [ ] **Step 2: 検証対象の銘柄に切り替える**

`chart_set_symbol` を引数 `{ "symbol": "TSE:5016" }` で呼ぶ。

Expected: `success: true`

シンボル形式がエラーになる場合は `symbol_search` を引数 `{ "query": "5016" }` で呼び、
返された正式なシンボル文字列を使う。

- [ ] **Step 3: 指標値を取得する**

`data_get_study_values` を引数なしで呼ぶ。

Expected: チャート上の全インジケーターの現在値。
少なくとも Supertrend と SQZMOM_LB の数値が含まれること

これが **判定項目 3** の結果になる。両方とも数値が取れなければ NG。

- [ ] **Step 4: 取得結果を保存する**

Step 1 と Step 3 の戻り値を、以下のファイルに JSON として書き出す。

- `data/tradingview/state/chart_state_5016.json`
- `data/tradingview/state/study_values_5016.json`

- [ ] **Step 5: 価格データも取得して J-Quants と突き合わせる**

`data_get_ohlcv` を引数 `{ "summary": true, "count": 20 }` で呼ぶ。

あわせてローカルの J-Quants データと比較する。

```bash
cd ~/stock_analysis && .venv/Scripts/python.exe scripts/deep_analysis_helper.py 5016 | head -40
```

Expected: 直近終値が概ね一致する（TradingView 側は 20 分遅延のため、ザラ場中は差が出る。
市場クローズ時なら一致するはず）

乖離が大きい場合は分割調整の基準が違う可能性があるため、差分を記録しておく。

---

### Task 6: Pine 描画データの取得

- [ ] **Step 1: 描画された価格線を取得する**

`data_get_pine_lines` を引数なしで呼ぶ。

Expected: インジケーターが `line.new()` で描いた水平線の価格レベル一覧

- [ ] **Step 2: ラベルを取得する**

`data_get_pine_labels` を引数なしで呼ぶ。

Expected: チャート上の Buy / Sell ラベルなど、テキストと価格の組

ユーザーのチャートには Buy / Sell ラベルが表示されているため、これが取れるかが焦点。

- [ ] **Step 3: テーブルとボックスも確認する**

`data_get_pine_tables` と `data_get_pine_boxes` をそれぞれ引数なしで呼ぶ。

Expected: 該当する描画がなければ空配列。エラーにならないこと

- [ ] **Step 4: 判定する**

Step 1-2 のいずれかで意味のある値（実際のチャート表示と対応する数値やラベル）が
取れていれば **判定項目 4** は OK。両方とも空またはエラーなら NG。

- [ ] **Step 5: 結果を保存する**

取得結果を `data/tradingview/state/pine_graphics_5016.json` に書き出す。

---

### Task 7: CLI の非対話出力確認

- [ ] **Step 1: コマンド一覧を確認する**

```bash
cd ~/tradingview-mcp && node src/cli/index.js --help
```

Expected: 利用可能なコマンドの一覧が表示される

- [ ] **Step 2: 状態取得コマンドを実行する**

Step 1 で表示された一覧から状態取得系のコマンド（`status` など）を選び実行する。

```bash
cd ~/tradingview-mcp && node src/cli/index.js status
```

Expected: 接続状態が出力される

- [ ] **Step 3: JSON 出力が可能か確認する**

Step 1 の `--help` 出力に `--json` 相当のオプションがあればそれを付けて実行する。

```bash
cd ~/tradingview-mcp && node src/cli/index.js status --json
```

Expected: パース可能な JSON が標準出力に返る

- [ ] **Step 4: パイプ可能か確認する**

```bash
cd ~/tradingview-mcp && node src/cli/index.js status --json | node -e "let s='';process.stdin.on('data',d=>s+=d).on('end',()=>{const o=JSON.parse(s);console.log('parsed ok, keys:',Object.keys(o).join(','))})"
```

Expected: `parsed ok, keys: ...` が表示される

これが **判定項目 5** の結果になる。OK なら A-1 で Python 連携（`subprocess` 経由）が可能になり、
既存の `scripts/deep_analysis_helper.py` と同じ形に揃えられる。

---

### Task 8: 設定の退避

- [ ] **Step 1: ウォッチリストを取得する**

`watchlist_get` を引数なしで呼ぶ。

Expected: 登録銘柄の一覧。ユーザーのチャート画面にある「01_指数等」リストが含まれるはず

- [ ] **Step 2: レイアウト一覧を取得する**

`layout_list` を引数なしで呼ぶ。

Expected: 保存済みレイアウトの名前一覧

- [ ] **Step 3: 退避ファイルとして保存する**

取得結果を以下に書き出す。

- `data/tradingview/backup/watchlist_YYYY-MM-DD.json`
- `data/tradingview/backup/layouts_YYYY-MM-DD.json`

`YYYY-MM-DD` は実行日に置き換える。

- [ ] **Step 4: Pine ソースは取得しないことを確認する**

`pine_list_scripts` は**呼ばない**。
設計書の通り、利用者の保有スクリプトは第三者製であり、取得も保存も行わない。

もし A-1 以降で自作スクリプトを作成した場合は、その時点で退避対象に加える。

- [ ] **Step 5: 保存内容を確認する**

```bash
ls -la ~/stock_analysis/data/tradingview/backup/ && cd ~/stock_analysis && git status --short
```

Expected: ファイルが存在し、かつ `git status` には現れない（除外が効いている）

---

### Task 9: CDP 終了と復旧確認

- [ ] **Step 1: TradingView を終了する**

```bash
taskkill //F //IM TradingView.exe
```

Expected: 終了メッセージ。プロセスが見つからない場合はすでに終了している

- [ ] **Step 2: ポートが閉じたことを確認する**

```bash
netstat -ano | grep 9222 || echo "port 9222 closed"
```

Expected: `port 9222 closed` が表示される

- [ ] **Step 3: 通常起動する**

ユーザーにスタートメニューから通常どおり TradingView を起動してもらう。
（MSIX フォールバックでローカルコピーが作られた場合、そちらではなく
本来の TradingView を起動することを確認する）

- [ ] **Step 4: 復旧を確認する**

ユーザーに以下を確認してもらう。

- ログイン状態が保持されているか
- チャートのレイアウトとインジケーターが元通りか
- ウォッチリストが残っているか

これが **判定項目 6 の後半と判定項目 7** の結果になる。

- [ ] **Step 5: CDP が閉じていることを再確認する**

```bash
netstat -ano | grep 9222 || echo "port 9222 closed"
```

Expected: `port 9222 closed`（通常起動では CDP は有効にならない）

---

### Task 10: 判定表の記入と設計書への追記

**Files:**
- Modify: `docs/superpowers/specs/2026-09-12-tradingview-integration-design.md`

- [ ] **Step 1: 判定結果をまとめる**

Task 3-9 で得た結果を 8 項目の判定表に埋める。
判定項目 8（再現性）は、検証中に SHA が `c05b8f5` のまま変わっていないことを確認して記入する。

```bash
cd ~/tradingview-mcp && git rev-parse HEAD
```

Expected: `c05b8f5755ed8e64ea242de88ddbf46aa24d56a4`

- [ ] **Step 2: 設計書に「A-0 実施結果」節を追記する**

設計書の「## A-1 方向性（A-0 の結果で確定）」の**直前**に、以下の構成で追記する。

```markdown
## A-0 実施結果（YYYY-MM-DD）

### 判定表

| # | 判定項目 | 結果 | 備考 |
|---|---|---|---|
| 1 | MSIX 環境での起動 | OK / NG | |
| 2 | CDP バインド | OK / NG | |
| 3 | 指標値の取得 | OK / NG | 取得できた指標名を列挙 |
| 4 | Pine 描画の取得 | OK / NG | |
| 5 | CLI の非対話出力 | OK / NG | JSON オプションの有無 |
| 6 | 起動済み TradingView の復旧 | OK / NG | |
| 7 | CDP 終了後の復旧 | OK / NG | |
| 8 | 再現性 | OK / NG | 固定 SHA |

### 取得できたデータ

（`data_get_study_values` で実際に読めた指標名と値の例を記す）

### 取得できなかったもの・制約

（エラーになったツール、空を返したツール、想定と違った挙動を記す）

### A-1 への申し送り

（実装形態の案 1/2/3 のどれが選べるか、CLI 連携の可否を踏まえて記す）
```

実際の結果に置き換えて記入する。OK / NG はどちらか一方を残す。

- [ ] **Step 3: 撤退条件に該当するか判定する**

- 判定項目 1 または 6 が NG → 系統 A を中止し、設計書の「保留事項」に中止理由を記して B-1 へ切り替える
- 判定項目 3 と 4 が両方 NG → A-1 を中止し、その旨を記す
- いずれにも該当しない → A-1 の詳細設計へ進む

- [ ] **Step 4: コミット**

```bash
cd ~/stock_analysis && git add docs/superpowers/specs/2026-09-12-tradingview-integration-design.md && git commit -m "docs: A-0検証結果を設計書に追記

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

- [ ] **Step 5: ユーザーに報告する**

判定表の結果と、A-1 に進めるかどうかの判断を伝える。
CDP が閉じていること、TradingView が通常状態に戻っていることもあわせて報告する。
