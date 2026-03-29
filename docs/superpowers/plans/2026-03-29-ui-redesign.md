# UI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `styles/custom.css` と `app.py` を更新し、アプリ全体を「Modern Dark Fintech」スタイルに統一する。

**Architecture:** CSSの全面更新（カラートークン・コンポーネントスタイル）と `app.py` のインラインスタイル定数・カード一覧の更新のみ。ページ機能・ロジックは一切変更しない。

**Tech Stack:** CSS, Python 3.9.1, Streamlit

---

## ファイル構成

| ファイル | 変更内容 |
|---------|---------|
| `styles/custom.css` | 全面書き換え（カラートークン適用、コンポーネント追加） |
| `app.py` | インラインスタイル定数更新 + トレードログカード追加（5→6列） |

---

## Task 1: `styles/custom.css` 全面更新

**Files:**
- Modify: `styles/custom.css`

- [ ] **Step 1: `styles/custom.css` を以下の内容で完全に置き換える**

```css
/* ============================================
   サイドバー
   ============================================ */
section[data-testid="stSidebar"] { min-width: 320px; max-width: 320px; }

section[data-testid="stSidebar"] > div:first-child {
    background: #0d1117;
    border-right: 1px solid #30363d;
}

/* ============================================
   メトリクスカード
   ============================================ */
[data-testid="stMetric"] {
    background: #161b22;
    border-radius: 8px;
    padding: 14px 16px;
    border: 1px solid #30363d;
    border-left: 3px solid #2196f3;
    box-shadow: 0 2px 8px rgba(0,0,0,0.4);
}

[data-testid="stMetricLabel"] p { color: #8b949e; font-size: 0.82rem; }
[data-testid="stMetricValue"]   { color: #e6edf3; font-weight: 700; }

/* ============================================
   スコアバッジ
   ============================================ */
.score-high {
    background: linear-gradient(135deg, #27ae60, #1e8449);
    color: white; padding: 3px 10px; border-radius: 12px;
    font-weight: bold; font-size: 0.82rem;
}
.score-mid {
    background: linear-gradient(135deg, #f39c12, #d68910);
    color: white; padding: 3px 10px; border-radius: 12px;
    font-weight: bold; font-size: 0.82rem;
}
.score-low {
    background: linear-gradient(135deg, #e74c3c, #c0392b);
    color: white; padding: 3px 10px; border-radius: 12px;
    font-weight: bold; font-size: 0.82rem;
}

/* ============================================
   見出し
   ============================================ */
h1 { color: #e6edf3; border-bottom: 2px solid #2196f3; padding-bottom: 8px; margin-bottom: 20px; }
h2 { color: #e6edf3; }
h3 { color: #e6edf3; }

/* ============================================
   タブ
   ============================================ */
[data-testid="stTabs"] [role="tab"] {
    color: #8b949e;
    font-weight: 500;
}
[data-testid="stTabs"] [role="tab"][aria-selected="true"] {
    color: #e6edf3;
    border-bottom-color: #2196f3 !important;
}

/* ============================================
   データフレーム — 奇数行ストライプ
   ============================================ */
[data-testid="stDataFrame"] tbody tr:nth-child(odd) td {
    background: rgba(255,255,255,0.02) !important;
}

/* ============================================
   Plotly ツールバー位置調整
   ============================================ */
.modebar-container { top: -32px !important; }

/* ============================================
   モバイル対応 (max-width: 768px)
   ============================================ */
@media (max-width: 768px) {
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.0rem !important; }

    section[data-testid="stSidebar"] {
        min-width: 0 !important;
        max-width: 80vw !important;
    }

    [data-testid="column"] {
        width: 100% !important;
        flex: 1 1 100% !important;
        min-width: 100% !important;
    }

    .block-container {
        padding-left: 2rem !important;
        padding-right: 2rem !important;
        padding-top: 2rem !important;
    }

    button[kind="primary"], button[kind="secondary"] {
        min-height: 44px !important;
    }

    [data-testid="stDataFrame"] {
        overflow-x: auto !important;
    }

    [data-testid="stMetric"] {
        padding: 8px !important;
    }
}
```

- [ ] **Step 2: 構文チェック（CSSはバリデーターが不要のため、行数確認で代替）**

```bash
cd C:\Users\ryohei\stock_analysis && python -c "
with open('styles/custom.css', encoding='utf-8') as f:
    content = f.read()
assert 'score-high' in content
assert 'stMetric' in content
assert '#0d1117' in content
assert '#161b22' in content
assert '#30363d' in content
assert 'linear-gradient' in content
print('OK:', len(content), 'chars')
"
```

Expected: `OK: <数値> chars`

- [ ] **Step 3: コミット**

```bash
cd C:\Users\ryohei\stock_analysis && git add styles/custom.css && git commit -m "style: Modern Dark Fintech — custom.css全面更新"
```

---

## Task 2: `app.py` インラインスタイル + トレードログカード追加

**Files:**
- Modify: `app.py`

- [ ] **Step 1: `app.py` の定数4行とカードリスト・列定義を更新する**

`app.py` の `CARD_STYLE` から `for col` ループまでを以下に置き換える（現在は5列・5カード）:

```python
CARD_STYLE = (
    "background:#161b22; border-radius:12px; padding:24px;"
    "border:1px solid #30363d; border-top:3px solid {color};"
    "min-height:160px; box-shadow:0 2px 8px rgba(0,0,0,0.4);"
)
ICON_STYLE = (
    "width:42px; height:42px; border-radius:10px;"
    "background:{color}26; display:inline-flex; align-items:center;"
    "justify-content:center; font-size:22px; margin-bottom:14px;"
)
TITLE_STYLE = "color:#e6edf3; margin:0 0 8px 0; font-size:1.05rem; font-weight:600;"
DESC_STYLE  = "color:#8b949e; font-size:0.84rem; line-height:1.6; margin:0;"

cards = [
    ("pages/1_screening.py", "スクリーニングを開始", "#2196f3", "⚡", "スクリーニング",
     "PER・PBR・ROE・配当利回り・テクニカル指標などを組み合わせて割安・好業績株をスクリーニングします。"),
    ("pages/2_stock_detail.py", "銘柄詳細を見る", "#26a69a", "📈", "銘柄詳細",
     "個別銘柄の株価チャート・財務指標・適時開示情報を一画面で確認できます。"),
    ("pages/3_disclosures.py", "適時開示を確認", "#ff9800", "📰", "適時開示",
     "TDnetの適時開示情報を最新順・日付指定・銘柄コード別で検索・閲覧できます。"),
    ("pages/4_portfolio.py", "ポートフォリオを見る", "#9c27b0", "💹", "ポートフォリオ",
     "SBI証券のCSVをインポートして保有状況・含み損益・セクター分散をグラフで確認できます。"),
    ("pages/5_portfolio_analysis.py", "AI分析を実行", "#e91e63", "🤖", "AI分析",
     "Claude AIが保有銘柄をファンダ・テクニカル両面から分析し、売買提案とアクションを提示します。"),
    ("pages/6_trade_log.py", "トレードを記録", "#2e7d32", "📓", "トレードログ",
     "実トレードを記録・集計し、戦略別勝率・RSI別成績など自己分析データを蓄積します。"),
]

col1, col2, col3, col4, col5, col6 = st.columns(6)

for col, (page, label, color, icon, title, desc) in zip([col1, col2, col3, col4, col5, col6], cards):
    with col:
        st.markdown(f"""
        <div style="{CARD_STYLE.format(color=color)}">
            <div style="{ICON_STYLE.format(color=color)}">{icon}</div>
            <h3 style="{TITLE_STYLE}">{title}</h3>
            <p style="{DESC_STYLE}">{desc}</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link(page, label=label)
```

- [ ] **Step 2: サイドバーにトレードログのリンクを追加する**

`app.py` の末尾付近、`st.sidebar.page_link("pages/5_portfolio_analysis.py", ...)` の直後に追加:

```python
st.sidebar.page_link("pages/6_trade_log.py", label="トレードログ", icon="📓")
```

- [ ] **Step 3: 構文チェック**

```bash
cd C:\Users\ryohei\stock_analysis && .venv\Scripts\python.exe -c "import py_compile; py_compile.compile('app.py', doraise=True); print('OK')"
```

Expected: `OK`

- [ ] **Step 4: アプリ起動確認**

```bash
cd C:\Users\ryohei\stock_analysis && .venv\Scripts\python.exe -c "
import ast, sys
with open('app.py', encoding='utf-8') as f:
    src = f.read()
ast.parse(src)
assert '#161b22' in src, 'カード背景色が更新されていない'
assert '#30363d' in src, 'ボーダー色が更新されていない'
assert '#e6edf3' in src, 'テキスト色が更新されていない'
assert '6_trade_log' in src, 'トレードログカードがない'
assert 'col6' in src, '6列になっていない'
print('All assertions passed')
"
```

Expected: `All assertions passed`

- [ ] **Step 5: コミット**

```bash
cd C:\Users\ryohei\stock_analysis && git add app.py && git commit -m "style: ホームカード6列化・トレードログ追加・カラー統一"
```

---

## 自己レビュー

**Spec coverage チェック:**
- ✅ `bg-base: #0d1117` → `custom.css` サイドバー背景に適用
- ✅ `bg-card: #161b22` → `app.py` CARD_STYLE と `custom.css` stMetric に適用
- ✅ `border: #30363d` → 両ファイルに適用
- ✅ `text-primary: #e6edf3` → `app.py` TITLE_STYLE と h1/h2/h3 に適用
- ✅ `text-secondary: #8b949e` → `app.py` DESC_STYLE と stMetricLabel に適用
- ✅ スコアバッジ グラデーション化 → Task 1 CSS
- ✅ データフレーム ストライプ → Task 1 CSS
- ✅ タブスタイル → Task 1 CSS
- ✅ トレードログカード追加 → Task 2
- ✅ サイドバーリンク追加 → Task 2
- ✅ モバイル対応は現状維持 → Task 1 CSS の `@media` ブロック

**Placeholder scan:** なし

**Type consistency:** CSS クラス名・HTML属性は仕様書と一致
