"""LLM Wiki frontmatter ギャップ一括補完スクリプト.

lint-report-2026-05-23.md の P2 対応として 107 ページの frontmatter を補う:
- entity / concept / question pages: status を追加 (default: developing)
- source pages: status: summarized, created (date フィールド or mtime), updated (mtime)
- meta / domain pages: status を追加 (default: active)

既存フィールドは上書きしない. type フィールドのないページはスキップ.
"""
from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

VAULT = Path(r"C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki")

DEFAULTS = {
    "entity": {"status": "developing"},
    "source": {"status": "summarized"},
    "concept": {"status": "developing"},
    "meta": {"status": "active"},
    "domain": {"status": "active"},
    "question": {"status": "developing"},
}


def parse_frontmatter(text: str):
    if not text.startswith("---\n"):
        return None, text
    end = text.find("\n---\n", 4)
    if end < 0:
        return None, text
    return text[4:end], text[end + 5 :]


def get_field(fm: str, key: str):
    m = re.search(rf"^{re.escape(key)}:\s*(.*)$", fm, re.MULTILINE)
    return m.group(1).strip() if m else None


def add_field(fm: str, key: str, value: str) -> str:
    return fm.rstrip("\n") + f"\n{key}: {value}"


def main(dry_run: bool = False) -> None:
    changed = 0
    skipped = 0
    summary = {"entity": 0, "source": 0, "concept": 0, "meta": 0, "domain": 0, "question": 0}

    for md_path in VAULT.rglob("*.md"):
        text = md_path.read_text(encoding="utf-8")
        fm, body = parse_frontmatter(text)
        if fm is None:
            skipped += 1
            continue

        type_val = get_field(fm, "type")
        if not type_val:
            skipped += 1
            continue
        type_val = type_val.strip().strip('"').strip("'").strip()

        if type_val not in DEFAULTS:
            skipped += 1
            continue

        new_fm = fm
        modified_fields = []

        if get_field(new_fm, "status") is None:
            new_fm = add_field(new_fm, "status", DEFAULTS[type_val]["status"])
            modified_fields.append("status")

        if type_val == "source":
            mtime_str = datetime.fromtimestamp(md_path.stat().st_mtime).strftime("%Y-%m-%d")
            date_val = get_field(new_fm, "date")
            if get_field(new_fm, "created") is None:
                created_val = (date_val.strip() if date_val else mtime_str)
                new_fm = add_field(new_fm, "created", created_val)
                modified_fields.append("created")
            if get_field(new_fm, "updated") is None:
                new_fm = add_field(new_fm, "updated", mtime_str)
                modified_fields.append("updated")

        if not modified_fields:
            continue

        if not dry_run:
            new_text = f"---\n{new_fm.rstrip(chr(10))}\n---\n{body}"
            md_path.write_text(new_text, encoding="utf-8")

        changed += 1
        summary[type_val] = summary.get(type_val, 0) + 1
        rel = md_path.relative_to(VAULT)
        print(f"  [{type_val}] {rel} -> {', '.join(modified_fields)}")

    print()
    print(f"Total updated: {changed} files (skipped {skipped} non-typed)")
    print(f"By type: {summary}")
    if dry_run:
        print("(dry run - no files written)")


if __name__ == "__main__":
    dry = "--dry-run" in sys.argv
    main(dry_run=dry)
