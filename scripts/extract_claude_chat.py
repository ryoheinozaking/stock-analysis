#!/usr/bin/env python3
"""Extract a Claude.ai chat conversation + its artifacts from a conversations.json export.

Usage:
    python scripts/extract_claude_chat.py <conversations.json> <uuid_prefix> <output_dir>

Outputs:
    <output_dir>/conversation.md          - chat content (text + tool_use/result summaries)
    <output_dir>/artifacts/<filename>     - final state of each user-facing artifact
                                             (str_replace edits applied in sequence)

Filters out dev/temp files: preview_page*.png, make_pdf*.py, debug*.py, *.tgz, etc.
"""
import json
import sys
import re
from pathlib import Path
from typing import Dict, List


DEV_FILE_PATTERNS = [
    r"preview_page.*\.png$",
    r"make_pdf.*\.py$",
    r"^debug.*\.py$",
    r"\.tgz$",
    r"^package/",
    r"chart\.umd\.js$",
    r"chart\.js-.*\.tgz$",
]


def is_dev_file(path: str) -> bool:
    name = Path(path).name
    for pat in DEV_FILE_PATTERNS:
        if re.search(pat, name) or re.search(pat, path):
            return True
    return False


def extract_artifacts(conv: dict) -> Dict[str, str]:
    """Walk create_file + str_replace + visualize:show_widget calls in order, return {filename: final_content}.

    Handles 3 artifact-producing tools:
    - create_file: saves file_text content to specified path
    - str_replace: applies edits in sequence
    - visualize:show_widget: saves widget_code (HTML/JS) as widget-{title}.html
    """
    files: Dict[str, str] = {}
    widget_counter: Dict[str, int] = {}
    for m in conv["chat_messages"]:
        for block in m.get("content", []):
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype != "tool_use":
                continue
            name = block.get("name", "")
            inp = block.get("input", {}) or {}
            if name == "create_file":
                path = inp.get("path", "")
                text = inp.get("file_text", "")
                if path and not is_dev_file(path):
                    files[path] = text
            elif name == "str_replace":
                path = inp.get("path", "")
                old = inp.get("old_str", "")
                new = inp.get("new_str", "")
                if path in files and old:
                    files[path] = files[path].replace(old, new, 1)
            elif name == "visualize:show_widget":
                title = inp.get("title", "widget")
                widget_code = inp.get("widget_code", "")
                # Skip obvious test widgets
                if "test" in title.lower() and len(widget_code) < 2000:
                    continue
                if not widget_code:
                    continue
                # Latest version of same-title widget wins (overwrite)
                widget_counter[title] = widget_counter.get(title, 0) + 1
                # Use just title (latest version overwrites earlier) as filename
                safe_title = "".join(c if c.isalnum() or c in "_-" else "_" for c in title)
                fname = f"widget-{safe_title}.html"
                files[fname] = widget_code
    return files


def render_conversation(conv: dict) -> str:
    """Render conversation as markdown. Skip thinking blocks; summarize tool calls."""
    lines = [
        f"# {conv['name']}",
        "",
        f"- uuid: {conv['uuid']}",
        f"- created: {conv['created_at']}",
        f"- updated: {conv['updated_at']}",
        f"- messages: {len(conv['chat_messages'])}",
        "",
    ]
    for i, m in enumerate(conv["chat_messages"]):
        sender = m.get("sender", "?")
        lines.append(f"## msg[{i}] — {sender}")
        lines.append("")
        for block in m.get("content", []):
            if not isinstance(block, dict):
                continue
            btype = block.get("type")
            if btype == "text":
                txt = (block.get("text") or "").strip()
                if txt:
                    lines.append(txt)
                    lines.append("")
            elif btype == "thinking":
                pass  # skip internal reasoning
            elif btype == "tool_use":
                tname = block.get("name", "?")
                tinput = block.get("input", {}) or {}
                # For create_file/str_replace, point to artifacts/ dir instead of dumping content
                if tname == "create_file":
                    path = tinput.get("path", "")
                    fname = Path(path).name
                    desc = tinput.get("description", "")
                    if is_dev_file(path):
                        lines.append(f"> 🔧 **[Tool call]** `create_file` (dev file, not saved) `{fname}` — {desc}")
                    else:
                        lines.append(f"> 📄 **[Artifact created]** `artifacts/{fname}` — {desc}")
                elif tname == "str_replace":
                    path = tinput.get("path", "")
                    fname = Path(path).name
                    desc = tinput.get("description", "")
                    if is_dev_file(path):
                        lines.append(f"> ✏️  **[Tool call]** `str_replace` (dev file, not saved) `{fname}` — {desc}")
                    else:
                        lines.append(f"> ✏️  **[Artifact edited]** `artifacts/{fname}` — {desc}")
                elif tname == "visualize:show_widget":
                    title = tinput.get("title", "widget")
                    wc_size = len(tinput.get("widget_code", ""))
                    if "test" in title.lower() and wc_size < 2000:
                        lines.append(f"> 📊 **[Widget shown]** `{title}` (test, not saved) {wc_size} chars")
                    else:
                        safe_title = "".join(c if c.isalnum() or c in "_-" else "_" for c in title)
                        lines.append(f"> 📊 **[Widget rendered]** `artifacts/widget-{safe_title}.html` ({wc_size} chars)")
                else:
                    input_str = json.dumps(tinput, ensure_ascii=False)[:300]
                    lines.append(f"> 🔧 **[Tool call]** `{tname}` input=`{input_str}`")
                lines.append("")
            elif btype == "tool_result":
                content = block.get("content", "")
                if isinstance(content, list):
                    content = " ".join(
                        c.get("text", "") for c in content if isinstance(c, dict)
                    )
                content_str = str(content)[:500]
                lines.append(f"> 📤 **[Tool result]** {content_str}...")
                lines.append("")
    return "\n".join(lines)


def main():
    if len(sys.argv) != 4:
        print(__doc__, file=sys.stderr)
        sys.exit(1)

    json_path = Path(sys.argv[1])
    uuid_prefix = sys.argv[2]
    out_dir = Path(sys.argv[3])

    data = json.loads(json_path.read_text(encoding="utf-8"))
    matches = [c for c in data if c["uuid"].startswith(uuid_prefix)]
    if len(matches) != 1:
        print(f"ERROR: uuid prefix '{uuid_prefix}' matched {len(matches)} conversations", file=sys.stderr)
        sys.exit(1)
    conv = matches[0]

    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write conversation.md
    conv_md = render_conversation(conv)
    (out_dir / "conversation.md").write_text(conv_md, encoding="utf-8")

    # 2. Extract artifacts
    artifacts = extract_artifacts(conv)
    if artifacts:
        art_dir = out_dir / "artifacts"
        art_dir.mkdir(exist_ok=True)
        for path, content in artifacts.items():
            fname = Path(path).name
            (art_dir / fname).write_text(content, encoding="utf-8")
        print(f"  artifacts: {len(artifacts)} files saved to {art_dir}")
        for path in sorted(artifacts.keys()):
            print(f"    - {Path(path).name}")
    else:
        print("  no artifacts")

    print(f"conversation: {out_dir / 'conversation.md'} ({len(conv_md)} chars)")


if __name__ == "__main__":
    main()
