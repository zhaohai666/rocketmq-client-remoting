#!/usr/bin/env python3
"""strip_port_citations.py — - Remove cross-port source citations from comments in all seven ports (language names / other port paths / upstream source line numbers).

Only touches comments and docstrings; never code or ordinary string literals:
- A whole-line comment (// or #) that matches -> deleted at line granularity; a trailing comment that matches -> only the comment is removed, keeping the code.
- /* */ block comments and triple-quoted docstrings -> **paragraph-level** handling: if any line in a
  paragraph matches a cross-port source citation, the whole paragraph is deleted (to avoid leaving
  orphaned continuation fragments); paragraphs with no match are kept as-is.
- shebang-safe; if a line is left with only whitespace after deletion, the line is removed.

Match pattern: the upstream server source package name, plus trigger words from the compile-time
constant `TRIGGER_WORDS_RE`
(all matched with \b word boundaries, so near-identical words are never
accidentally hit). Class names belonging to this port (e.g. MQClientInstance in cpp) are not triggers.
(e.g. MQClientInstance in cpp) does not count as a trigger.

Usage: python3 scripts/strip_port_citations.py [--dry-run] [paths...]
"""

from __future__ import annotations

import argparse
import os
import re
import sys

TRIGGER_WORDS_RE = re.compile(r"org\.apache\.rocketmq|\bJava\b|\bJVM\b")

DEFAULT_ROOTS = [
    "python",
    "cpp/src", "cpp/include", "cpp/tests", "cpp/examples",
    "csharp/src", "csharp/tests", "csharp/examples",
    "php/src", "php/tests", "php/examples",
    "rust/src", "rust/examples",
    "go",
    "nodeJs/src", "nodeJs/test", "nodeJs/examples", "nodeJs/selfcheck.ts",
]

EXT_LINE_MARKERS = {
    ".py": ("#",),
    ".c": ("//",), ".h": ("//",), ".cc": ("//",), ".cpp": ("//",), ".hpp": ("//",),
    ".hh": ("//",),
    ".rs": ("//",),
    ".cs": ("//",),
    ".php": ("//",),
    ".go": ("//",),
    ".js": ("//",), ".ts": ("//",), ".mts": ("//",), ".cts": ("//",),
}

SKIP_DIR_PARTS = {"__pycache__", "node_modules", "target", "build", "bin", "obj", ".venv"}


def scan_line_regions(line: str) -> tuple[int | None, int | None, int | None]:
    """Return (line_comment_start, block_open, block_close) -- positions not wrapped in a string literal."""
    quote = ""
    i, n = 0, len(line)
    line_comment = None
    block_open = None
    block_close = None
    while i < n:
        ch = line[i]
        if quote:
            if ch == "\\" and quote in "\"'":
                i += 2
                continue
            if ch == quote:
                quote = ""
            i += 1
            continue
        if ch in "\"'":
            quote = ch
            i += 1
            continue
        if line_comment is None and line.startswith("//", i):
            line_comment = i
        if block_open is None and line.startswith("/*", i):
            block_open = i
        if block_close is None and line.startswith("*/", i):
            block_close = i
        i += 1
    return line_comment, block_open, block_close


def clean_line_comment_run(run: list[str], marker: str) -> list[str]:
    """Group consecutive pure line comments into paragraphs: a matching paragraph is deleted
    entirely; "sandwich" separator (blank) comment lines are kept.

    marker="//" also handles doc-comment variants like `///`, `//!`; marker="#" also handles `##`.
    """
    def is_sep(s: str) -> bool:
        body = s.strip()[len(marker):]
        return body.strip().strip("!/").strip() == ""

    items: list[tuple[str, object]] = []  # ('sep', line) | ('para', [lines])
    para: list[str] = []
    for s in run:
        if is_sep(s):
            if para:
                items.append(("para", para))
                para = []
            items.append(("sep", s))
        else:
            para.append(s)
    if para:
        items.append(("para", para))

    # null out matched paragraphs
    seq: list[tuple[str, object]] = []
    for kind, val in items:
        if kind == "para" and any(TRIGGER_WORDS_RE.search(x) for x in val):
            seq.append((kind, None))
        else:
            seq.append((kind, val))
    # keep separators only when both neighbors are kept paragraphs
    for idx, (kind, val) in enumerate(seq):
        if kind == "sep":
            before = any(k == "para" and v is not None for k, v in seq[:idx])
            after = any(k == "para" and v is not None for k, v in seq[idx + 1 :])
            if not (before and after):
                seq[idx] = (kind, None)
    res: list[str] = []
    for kind, val in seq:
        if val is None:
            continue
        res.extend(val if isinstance(val, list) else [val])
    return res


def process_c_like(lines: list[str]) -> tuple[list[str], int]:
    out: list[str] = []
    changed = 0
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        line_comment, block_open, block_close = scan_line_regions(line)

        # 1) block comment opens
        if block_open is not None and (line_comment is None or block_open < line_comment):
            # block comment starts on this line; collect until */
            block_lines: list[str] = []
            if block_close is not None:
                # single-line block comment: delete only if the whole comment matches
                comment_text = line[block_open : block_close + 2]
                if TRIGGER_WORDS_RE.search(comment_text):
                    changed += 1
                    head = line[:block_open].rstrip()
                    tail = line[block_close + 2 :].lstrip()
                    if head and tail:
                        out.append(head + " " + tail + ("\n" if line.endswith("\n") else ""))
                    elif head or tail:
                        out.append(head + tail + ("\n" if line.endswith("\n") else ""))
                    # both empty -> drop the whole line
                else:
                    out.append(line)
                i += 1
                continue
            # multi-line block comment -> collect until */; delete the whole block if any line matches
            # (the old paragraph-level kept the delimiter line, so trigger words on the /** opening line could never be deleted)
            j = i + 1
            closed = False
            block_lines.append(line)
            while j < n:
                _, _, bc = scan_line_regions(lines[j])
                block_lines.append(lines[j])
                if bc is not None:
                    closed = True
                    j += 1
                    break
                j += 1
            if TRIGGER_WORDS_RE.search("".join(block_lines)):
                changed += len(block_lines)
                first, last = block_lines[0], block_lines[-1]
                head = first[:block_open].rstrip()
                _, _, bc_last = scan_line_regions(last)
                tail = last[bc_last + 2 :].lstrip() if bc_last is not None else ""
                eol = "\n" if last.endswith("\n") else ""
                if head and tail:
                    out.append(head + " " + tail + eol)
                elif head or tail:
                    out.append((head or tail) + eol)
                i = j
                continue
            out.extend(block_lines)
            i = j
            continue

        # 2) line comments -- process consecutive pure comment lines at paragraph level first (avoid leaving orphaned continuation lines after line deletion)
        if line_comment is not None and line.strip().startswith("//"):
            k = i
            run: list[str] = []
            while k < n:
                s = lines[k]
                ls, _, _ = scan_line_regions(s)
                if ls is None or not s.strip().startswith("//"):
                    break
                run.append(s)
                k += 1
            cleaned = clean_line_comment_run(run, marker="//")
            changed += len(run) - len(cleaned)
            out.extend(cleaned)
            i = k
            continue

        # 3) trailing comment
        if line_comment is not None:
            code, comment = line[:line_comment], line[line_comment:]
            if TRIGGER_WORDS_RE.search(comment):
                changed += 1
                code_r = code.rstrip()
                if not code_r:
                    i += 1
                    continue  # drop the whole line
                out.append(code_r + ("\n" if line.endswith("\n") else ""))
                i += 1
                continue
            out.append(line)
            i += 1
            continue

        # 3) plain code line
        out.append(line)
        i += 1
    return out, changed


def clean_block_paragraphs(block_lines: list[str]) -> list[str]:
    """Group multi-line block comments into paragraphs (separated by consecutive non-decorative blank lines); delete a whole paragraph if it matches."""
    # decompose each line into (lead, star, text, raw)
    def decorate(idx: int, raw: str, keep: bool) -> str:
        if keep:
            return raw
        return ""

    entries: list[tuple[str, bool]] = []  # (raw, is_blank_decor)
    for raw in block_lines:
        stripped = raw.strip()
        core = stripped
        if core.startswith("/*"):
            core = core[2:]
        if core.endswith("*/"):
            core = core[:-2]
        core = core.lstrip("*").strip()
        entries.append((raw, core == ""))

    # paragraph grouping: separated by decorative blank lines
    paragraphs: list[list[int]] = []
    cur: list[int] = []
    for idx, (_, blank) in enumerate(entries):
        if blank:
            if cur:
                paragraphs.append(cur)
                cur = []
            paragraphs.append([idx])  # a blank line forms its own paragraph
        else:
            cur.append(idx)
    if cur:
        paragraphs.append(cur)

    keep_lines: list[str] = []
    for para in paragraphs:
        text = "".join(entries[k][0] for k in para)
        # keep blank lines as-is
        if all(entries[k][1] for k in para):
            keep_lines.extend(entries[k][0] for k in para)
            continue
        if TRIGGER_WORDS_RE.search(text):
            # delete the whole paragraph; if it contains block delimiters, keep the delimiter structure
            for k in para:
                raw = entries[k][0]
                if "/*" in raw or "*/" in raw:
                    keep_lines.append(raw)
            continue
        keep_lines.extend(entries[k][0] for k in para)
    return [x for x in keep_lines if x is not None]


def _block_becomes_empty(lines: list[str], start: int, end: int, indent_len: int) -> bool:
    """After removing the statement in range (start, end), check whether its enclosing block becomes empty.

    Look at the first non-empty line after end: if its indentation is deeper than the statement, the block
    still has content; otherwise the block is empty.
    """
    for k in range(end, len(lines)):
        s = lines[k].strip()
        if not s:
            continue
        return len(lines[k]) - len(lines[k].lstrip()) <= indent_len
    return True


def find_py_hash(line: str) -> int | None:
    """Return the position of the first # in the line not wrapped in a string literal (None if none)."""
    quote = ""
    i, n = 0, len(line)
    while i < n:
        ch = line[i]
        if quote:
            if ch == "\\" and quote in "\"'":
                i += 2
                continue
            if ch == quote:
                quote = ""
            i += 1
            continue
        if ch in "\"'":
            quote = ch
            i += 1
            continue
        if ch == "#":
            return i
        i += 1
    return None


def process_python(lines: list[str]) -> tuple[list[str], int]:
    out: list[str] = []
    changed = 0
    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        stripped = line.strip()

        # docstring
        if not stripped.startswith("#"):
            triple = line.count('"""') + line.count("'''")
            if triple:
                delim = '"""' if '"""' in line else "'''"
                if triple % 2 == 1:
                    # multi-line docstring opens: collect until closing
                    j = i + 1
                    block = [line]
                    closed = False
                    while j < n:
                        block.append(lines[j])
                        if delim in lines[j]:
                            closed = True
                            j += 1
                            break
                        j += 1
                    # any line matches -> delete the whole docstring (keeping delimiter or paragraph would let trigger words survive)
                    if any(TRIGGER_WORDS_RE.search(b) for b in block):
                        changed += len(block)
                        lead = line[: line.find(delim)]
                        prev = out[-1] if out else ""
                        if prev.rstrip().endswith(":") and _block_becomes_empty(
                            lines, i, j, len(lead)
                        ):
                            out.append(lead + "pass" + ("\n" if line.endswith("\n") else ""))
                        i = j
                        continue
                    cleaned = clean_docstring_block(block, delim)
                    changed += sum(1 for a, b in zip(block, cleaned) if a != b)
                    changed += len(block) - len(cleaned)
                    out.extend(cleaned)
                    i = j
                    continue
                # single-line docstring: on match -> delete the whole line; if it is the block's only content, add pass
                if TRIGGER_WORDS_RE.search(line):
                    changed += 1
                    lead = line[: line.find(delim)]
                    if lead.strip() == "":
                        prev = out[-1] if out else ""
                        if prev.rstrip().endswith(":") and _block_becomes_empty(
                            lines, i, i + 1, len(lead)
                        ):
                            out.append(lead + "pass" + ("\n" if line.endswith("\n") else ""))
                        i += 1
                        continue
                    out.append(lead + delim * 2 + ("\n" if line.endswith("\n") else ""))
                    i += 1
                    continue

        # consecutive pure # comment lines -> paragraph-level handling (avoid orphaned continuation lines)
        if stripped.startswith("#"):
            k = i
            run: list[str] = []
            while k < n:
                s = lines[k]
                if not s.strip().startswith("#"):
                    break
                run.append(s)
                k += 1
            cleaned = clean_line_comment_run(run, marker="#")
            changed += len(run) - len(cleaned)
            out.extend(cleaned)
            i = k
            continue

        # line comment (whole-line # or trailing #)
        hash_pos = find_py_hash(line)
        if hash_pos is not None:
            code, comment = line[:hash_pos], line[hash_pos:]
            if TRIGGER_WORDS_RE.search(comment):
                changed += 1
                code_r = code.rstrip()
                if not code_r:
                    i += 1
                    continue
                out.append(code_r + ("\n" if line.endswith("\n") else ""))
                i += 1
                continue
        out.append(line)
        i += 1
    return out, changed


def clean_docstring_block(block: list[str], delim: str) -> list[str]:
    """Group triple-quoted blocks by paragraph for deletion; keep the delimiter lines."""
    open_line, close_line = block[0], block[-1]
    body = block[1:-1] if len(block) > 2 else []
    # single block: conservatively skip complex cases like """...""" repeated on the same line
    if block[0] is block[-1] or len(block) < 2:
        return block
    kept: list[str] = [open_line]
    para: list[str] = []
    for raw in body:
        if raw.strip() == "":
            if para:
                if TRIGGER_WORDS_RE.search("".join(para)):
                    pass  # discard the whole paragraph
                else:
                    kept.extend(para)
                para = []
            kept.append(raw)
        else:
            para.append(raw)
    if para:
        if TRIGGER_WORDS_RE.search("".join(para)):
            pass
        else:
            kept.extend(para)
    kept.append(close_line)
    return kept


def iter_files(roots: list[str]):
    repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    for root in roots:
        full = os.path.join(repo, root)
        if os.path.isfile(full):
            yield full
            continue
        for dirpath, dirnames, filenames in os.walk(full):
            dirnames[:] = [d for d in dirnames if d not in SKIP_DIR_PARTS]
            for name in sorted(filenames):
                ext = os.path.splitext(name)[1]
                if ext in EXT_LINE_MARKERS:
                    yield os.path.join(dirpath, name)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("paths", nargs="*", default=DEFAULT_ROOTS)
    args = ap.parse_args()
    total = 0
    files_changed = 0
    for path in iter_files(args.paths):
        with open(path, encoding="utf-8") as fh:
            lines = fh.readlines()
        if path.endswith(".py"):
            cleaned, changed = process_python(lines)
        else:
            cleaned, changed = process_c_like(lines)
        if changed:
            files_changed += 1
            total += changed
            if not args.dry_run:
                with open(path, "w", encoding="utf-8") as fh:
                    fh.write("".join(cleaned))
            else:
                print(f"{changed:4d}  {os.path.relpath(path)}")
    print(f"{'[dry-run] ' if args.dry_run else ''}total {total} lines in {files_changed} files")
    return 0


if __name__ == "__main__":
    sys.exit(main())
