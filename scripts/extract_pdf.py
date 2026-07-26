"""PDF text extraction using PyMuPDF (fitz).

Use this for ALL PDFs in this project / LLM Wiki vault, especially Japanese earnings reports.

Why:
- `Read` tool requires pdftoppm (Windows poppler missing).
- `pypdf` / `pdfminer.six` / `pdfplumber` fail on Japanese (ToUnicode CMap missing)
  AND have a cryptography DLL bug on this Windows env (`_rust import error`).
- PyMuPDF bundles MuPDF, no external deps, handles Japanese CID fonts correctly.

Usage:
    .venv\\Scripts\\python.exe scripts\\extract_pdf.py "<pdf path>"

Output:
    Plain UTF-8 text to stdout. Each page separated by `\\n--- PAGE BREAK ---\\n`.

Exit codes:
    0 = success
    1 = bad arguments
    2 = file not found / open error
"""
from __future__ import annotations

import sys
from pathlib import Path


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


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: extract_pdf.py <pdf_path> [--save]", file=sys.stderr)
        print("  --save: also write text to <pdf_path>.txt sibling file", file=sys.stderr)
        return 1

    pdf_path = Path(sys.argv[1])
    save_sibling = "--save" in sys.argv[2:]

    if not pdf_path.exists():
        print(f"ERROR: File not found: {pdf_path}", file=sys.stderr)
        return 2

    try:
        import fitz  # noqa: F401  PyMuPDF (import check only; extract_text() does the real import)
    except ImportError:
        print(
            "ERROR: PyMuPDF not installed. Run:\n"
            "    .venv\\Scripts\\python.exe -m pip install --trusted-host pypi.org "
            "--trusted-host files.pythonhosted.org pymupdf",
            file=sys.stderr,
        )
        return 2

    try:
        full_text = extract_text(pdf_path)
    except Exception as e:
        print(f"ERROR: Cannot open PDF: {e}", file=sys.stderr)
        return 2

    # Ensure stdout uses utf-8 (Windows console default is cp932 → mojibake risk)
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    print(full_text)

    if save_sibling:
        txt_path = pdf_path.with_suffix(pdf_path.suffix + ".txt")
        txt_path.write_text(full_text, encoding="utf-8")
        print(f"\n[saved sibling: {txt_path}]", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
