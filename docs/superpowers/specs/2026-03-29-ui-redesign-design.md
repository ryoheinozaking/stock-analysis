# UI Redesign — Modern Dark Fintech

**Goal:** 既存のStreamlitアプリ全体のビジュアルを「Modern Dark Fintech」スタイルに統一する。機能・レイアウト・コード構造は変更しない。

**Architecture:** `styles/custom.css` の全面更新と `app.py` のインラインスタイル定数の調整が中心。各ページファイルには最小限の手を入れる（インラインスタイルの色定数のみ）。

**Tech Stack:** CSS, Python (Streamlit), HTML

---

## カラーシステム

| トークン | 値 | 用途 |
|---------|-----|------|
| `bg-base` | `#0d1117` | ページ背景 |
| `bg-card` | `#161b22` | カード・パネル背景 |
| `bg-input` | `#1c2128` | 入力フィールド背景 |
| `border` | `#30363d` | ボーダー全般 |
| `text-primary` | `#e6edf3` | 主要テキスト |
| `text-secondary` | `#8b949e` | 補助テキスト・ラベル |
| `accent-blue` | `#2196f3` | プライマリアクション・スクリーニング |
| `accent-teal` | `#26a69a` | 銘柄詳細 |
| `accent-orange` | `#ff9800` | 適時開示 |
| `accent-purple` | `#9c27b0` | ポートフォリオ |
| `accent-pink` | `#e91e63` | AI分析 |
| `accent-green` | `#2e7d32` | トレードログ |
| `positive` | `#3fb950` | 上昇・利益 |
| `negative` | `#f85149` | 下降・損失 |

---

## コンポーネント仕様

### メトリクスカード（`[data-testid="stMetric"]`）
- 背景: `#161b22`
- ボーダー: `1px solid #30363d`
- 左ボーダー: `3px solid #2196f3`
- 角丸: `8px`
- パディング: `14px 16px`
- ボックスシャドウ: `0 2px 8px rgba(0,0,0,0.4)`

### スコアバッジ（`.score-high` / `.score-mid` / `.score-low`）
- high: `background: linear-gradient(135deg, #27ae60, #1e8449)`
- mid: `background: linear-gradient(135deg, #f39c12, #d68910)`
- low: `background: linear-gradient(135deg, #e74c3c, #c0392b)`
- 共通: `padding: 3px 10px; border-radius: 12px; font-size: 0.82rem`

### データフレーム（`[data-testid="stDataFrame"]`）
- ヘッダー背景: `#161b22`
- ストライプ行: 奇数行に `background: rgba(255,255,255,0.02)`

### ボタン（Primary）
- 背景: `#1d6fe8`（青）→ hover: `#1a63d4`
- ボーダー: なし
- 角丸: `8px`
- フォントウェイト: `600`

### サイドバー
- 背景: `#0d1117`（本体と統一）
- ボーダー右: `1px solid #30363d`

### ページタイトル（`h1`）
- カラー: `#e6edf3`
- ボーダーボトム: `2px solid #2196f3`（現状維持）
- マージンボトム: `20px`

---

## ファイル別変更内容

### `styles/custom.css`（全面更新）
- カラー変数の統一（上記トークン適用）
- メトリクスカード、スコアバッジ、ボタン、サイドバー、h1/h2/h3、テーブル行のスタイル更新
- モバイル対応セクションは現状維持（機能的変更なし）

### `app.py`（インラインスタイル定数のみ）
- `CARD_STYLE` の背景色 → `#161b22`、ボーダー → `#30363d`
- `TITLE_STYLE` のカラー → `#e6edf3`
- `DESC_STYLE` のカラー → `#8b949e`
- トレードログカード（6番目）を追加: `("pages/6_trade_log.py", "トレードを記録", "#2e7d32", "📓", "トレードログ", "実トレードを記録・集計し、戦略別勝率・RSI別成績など自己分析データを蓄積します。")`
- `col1〜col6` に変更（5→6列）

---

## 変更しないもの
- ページの機能・レイアウト・ウィジェット構成
- 各ページファイルの主要ロジック
- モバイルレスポンシブ対応（`custom.css` の `@media` ブロック）
- Streamlit の `set_page_config` 設定

---

## 成功基準
- アプリ起動時にエラーなし
- 全6ページで統一されたダークテーマが適用されている
- ホーム画面にトレードログカードが表示されている
- チャート・テーブルの可読性が現状以上

