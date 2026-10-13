#!/usr/bin/env python3
"""strip_port_citations.py — 清理七端注释里的跨端出处引用（语言名 / 其他端口路径 / 上游源码行号）。

只动注释与文档字符串，绝不动代码与普通字符串字面量：
- 行注释（// 或 #）整段命中 → 行级删除；行尾注释命中 → 只去注释保留代码；
- /* */ 块注释与三引号文档字符串 → **段落级**处理：段落内任一行命中
  跨端出处引用就整段删除（避免留下孤儿续行碎片）；无命中的段落原样保留；
- shebang 安全；删除后整行只剩空白则删行。

命中模式：上游服务端源码包名，加上编译期常量 `TRIGGER_WORDS_RE` 里的触发词
（一律用 \\b 词边界匹配，天然不误伤形近词）。本端口自己的类名
（如 cpp 里的 MQClientInstance）不作为触发条件。

用法：python3 scripts/strip_port_citations.py [--dry-run] [paths...]
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
    """返回 (line_comment_start, block_open, block_close) —— 不被字符串字面量包住的位置。"""
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
    """连续纯行注释按段落分组：命中段整段删除；「夹心」空注释分隔行保留。

    marker="//" 时兼容 `///`、`//!` 等文档注释变体；marker="#" 兼容 `##`。
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

    # 命中段置空
    seq: list[tuple[str, object]] = []
    for kind, val in items:
        if kind == "para" and any(TRIGGER_WORDS_RE.search(x) for x in val):
            seq.append((kind, None))
        else:
            seq.append((kind, val))
    # 分隔行仅在前后都有保留段落时保留
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

        # 1) 块注释开启
        if block_open is not None and (line_comment is None or block_open < line_comment):
            # 块注释从本行开始；收集到 */ 为止
            block_lines: list[str] = []
            if block_close is not None:
                # 单行块注释：注释整体命中才删
                comment_text = line[block_open : block_close + 2]
                if TRIGGER_WORDS_RE.search(comment_text):
                    changed += 1
                    head = line[:block_open].rstrip()
                    tail = line[block_close + 2 :].lstrip()
                    if head and tail:
                        out.append(head + " " + tail + ("\n" if line.endswith("\n") else ""))
                    elif head or tail:
                        out.append(head + tail + ("\n" if line.endswith("\n") else ""))
                    # 双空则整行丢弃
                else:
                    out.append(line)
                i += 1
                continue
            # 多行块注释 → 收集到 */ 为止；任一行命中则整块删除
            # （旧版段落级保留定界行，导致 /** 开行上的触发词永远删不掉）
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

        # 2) 行注释 —— 连续的纯注释行先做段落级处理（避免触发行删除后留下孤儿续行）
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

        # 3) 行尾注释
        if line_comment is not None:
            code, comment = line[:line_comment], line[line_comment:]
            if TRIGGER_WORDS_RE.search(comment):
                changed += 1
                code_r = code.rstrip()
                if not code_r:
                    i += 1
                    continue  # 整行丢弃
                out.append(code_r + ("\n" if line.endswith("\n") else ""))
                i += 1
                continue
            out.append(line)
            i += 1
            continue

        # 3) 纯代码行
        out.append(line)
        i += 1
    return out, changed


def clean_block_paragraphs(block_lines: list[str]) -> list[str]:
    """多行块注释按段落（连续非装饰空行）分组；命中段落整段删除。"""
    # 分解每行为 (lead, star, text, raw)
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

    # 段落分组：装饰空行分隔
    paragraphs: list[list[int]] = []
    cur: list[int] = []
    for idx, (_, blank) in enumerate(entries):
        if blank:
            if cur:
                paragraphs.append(cur)
                cur = []
            paragraphs.append([idx])  # 空行自成一段
        else:
            cur.append(idx)
    if cur:
        paragraphs.append(cur)

    keep_lines: list[str] = []
    for para in paragraphs:
        text = "".join(entries[k][0] for k in para)
        # 空行原样保留
        if all(entries[k][1] for k in para):
            keep_lines.extend(entries[k][0] for k in para)
            continue
        if TRIGGER_WORDS_RE.search(text):
            # 整段删除；若段落含块定界符则保留定界符结构
            for k in para:
                raw = entries[k][0]
                if "/*" in raw or "*/" in raw:
                    keep_lines.append(raw)
            continue
        keep_lines.extend(entries[k][0] for k in para)
    return [x for x in keep_lines if x is not None]


def _block_becomes_empty(lines: list[str], start: int, end: int, indent_len: int) -> bool:
    """删除 (start, end) 区间的语句后，其所在块是否会变成空体。

    查看 end 之后第一个非空行：缩进比该语句更深 → 块里还有内容；否则块空了。
    """
    for k in range(end, len(lines)):
        s = lines[k].strip()
        if not s:
            continue
        return len(lines[k]) - len(lines[k].lstrip()) <= indent_len
    return True


def find_py_hash(line: str) -> int | None:
    """返回行内第一个不被字符串字面量包住的 # 的位置（无则 None）。"""
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

        # 文档字符串
        if not stripped.startswith("#"):
            triple = line.count('"""') + line.count("'''")
            if triple:
                delim = '"""' if '"""' in line else "'''"
                if triple % 2 == 1:
                    # 多行文档字符串开启：收集到闭合
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
                    # 任一行命中 → 整个 docstring 删除（保留定界符或段落会让触发词幸存）
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
                # 单行文档字符串：命中 → 整行删除；若它是块唯一内容需补 pass
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

        # 连续纯 # 注释行 → 段落级处理（避免孤儿续行）
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

        # 行注释（整行 # 或行尾 #）
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
    """三引号块按段落分组删除；保留定界符行。"""
    open_line, close_line = block[0], block[-1]
    body = block[1:-1] if len(block) > 2 else []
    # 单体块："""...""" 同一行重复出现等复杂场景保守跳过
    if block[0] is block[-1] or len(block) < 2:
        return block
    kept: list[str] = [open_line]
    para: list[str] = []
    for raw in body:
        if raw.strip() == "":
            if para:
                if TRIGGER_WORDS_RE.search("".join(para)):
                    pass  # 整段丢弃
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
