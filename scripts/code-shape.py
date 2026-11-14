#!/usr/bin/env python3
"""Code-shape limits: every tracked Rust file stays within the clean-code shape.

The standard is the library skill `clean-code` (`sprout skill show
clean-code`). For every git-tracked .rs file (outside .trees/ and target/)
this counts three metrics and fails when any is over its limit:

  non_test_lines     at most 800. Every line of the file, less the lines
                     inside test-only items (see below).
  string_errors      0. `Result<..., String>` types.
  history_comments   0. Comment lines narrating history: "used to" (not "is/
                     are/be used to"), "no longer", "moved to", "previously",
                     "formerly", "now lives", "before the split", "retired".

There is no baseline: vines started strict, so no file is grandfathered.
`#[allow]` without a reason is a clippy lint here (`allow_attributes`,
`allow_attributes_without_reason` in Cargo.toml), so this script does not
count it.

Test-only items. An item is test-only when one of its outer `#[cfg(...)]`
attributes is false whenever `test` is off: `cfg(test)`, `cfg(all(test, unix))`,
nested `all`, `not(not(test))`. `cfg(any(test, feature = "x"))`, `cfg(not(test))`
and `cfg_attr(test, ...)` are not gates: that code compiles into non-test
builds. The gate is read wherever an item can sit: the file, inline `mod`
bodies, `impl`, `trait` and `extern` blocks, at any depth, for any item kind.
The item spans its outer doc comments and attributes through its closing `}`
or `;`, found on the token stream, so a brace in a string, raw string, char or
comment does not move it. A `#![cfg(test)]` inner attribute makes its module
test-only. A `cfg` on a statement, field, match arm or expression, or inside a
macro invocation, is not read: those lines count.

A line is test code when every token on it belongs to a test-only item, or it
has no token (blank, a plain comment, the middle of a multi-line literal) and
lies inside one. Blank and comment lines outside test items count: a reader
still holds them.

Test files count 0 non-test lines: files under a `tests/` directory,
`tests.rs`, `*_tests.rs`, a file whose `#![cfg(test)]` gates it whole, and a
file every `mod name;` declaration of which is test-only or sits in a test
file. A file that a production module also loads, or that no declaration
reaches (a crate root), counts.

The other two metrics count the whole file, test code and test files
included: a stringly error or a history comment is debt in a test too.
Comments and string literals are not code, and code is not a comment, so
`"Result<T, String>"` in a string counts nothing.

Exit status: 0 every file within its limits, 1 a limit exceeded, 2 bad input
(not a git checkout, git failed).
"""

import argparse
import collections
import posixpath
import re
import subprocess
import sys
from pathlib import Path

METRICS = ("non_test_lines", "string_errors", "history_comments")
LIMITS = {"non_test_lines": 800, "string_errors": 0, "history_comments": 0}
# Metrics found line by line; each finding is reported with its line number.
LINE_FOUND = ("string_errors", "history_comments")
EXCLUDED_TOP_DIRS = {".trees", "target"}

RESULT_OPEN = re.compile(r"\bResult\s*(?:::\s*)?<")
STRING_TYPES = {"String", "std::string::String", "::std::string::String", "alloc::string::String"}
HISTORY = re.compile(
    r"\b(?:(?<!\bis )(?<!\bare )(?<!\bbe )(?<!\bbeen )(?<!\bbeing )(?<!\bget )(?<!\bgets )(?<!\bgot )used to"
    r"|no longer|moved to|previously|formerly|now lives|before the split|retired)\b",
    re.IGNORECASE,
)


class InputError(Exception):
    """The checkout cannot be read as the check needs."""


# --- Measuring one file ----------------------------------------------------


def split_code_and_comments(text):
    """Return per-line (code, comment) pairs.

    String and char literal contents are dropped from code (the quotes stay),
    comments go to the comment side, nested block comments included.
    """
    lines = []
    code, comment = [], []
    i, n = 0, len(text)

    def flush():
        lines.append(("".join(code), "".join(comment)))
        code.clear()
        comment.clear()

    while i < n:
        c = text[i]
        if c == "\n":
            flush()
            i += 1
        elif text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end == -1 else end
            comment.append(text[i:end])
            i = end
        elif text.startswith("/*", i):
            depth, i = 1, i + 2
            comment.append("/*")
            while i < n and depth:
                if text.startswith("/*", i):
                    depth, i = depth + 1, i + 2
                    comment.append("/*")
                elif text.startswith("*/", i):
                    depth, i = depth - 1, i + 2
                    comment.append("*/")
                elif text[i] == "\n":
                    flush()
                    i += 1
                else:
                    comment.append(text[i])
                    i += 1
        elif c == "r" and _raw_string_start(text, i):
            j = i + 1
            hashes = 0
            while text[j] == "#":
                hashes, j = hashes + 1, j + 1
            close = '"' + "#" * hashes
            code.append('r"')
            i = j + 1
            while i < n and not text.startswith(close, i):
                if text[i] == "\n":
                    flush()
                i += 1
            code.append('"')
            i += len(close)
        elif c == '"':
            code.append('"')
            i += 1
            while i < n and text[i] != '"':
                if text[i] == "\\":
                    i += 1
                if i < n and text[i] == "\n":
                    flush()
                i += 1
            code.append('"')
            i += 1
        elif c == "'":
            end = _char_literal_end(text, i)
            if end is None:
                code.append(c)  # a lifetime or label
                i += 1
            else:
                code.append("''")
                i = end
        else:
            code.append(c)
            i += 1
    if code or comment:
        flush()
    return lines


def _raw_string_start(text, i):
    if i > 0 and (text[i - 1].isalnum() or text[i - 1] == "_") and text[i - 1] not in "bc":
        return False
    if i > 1 and text[i - 1] in "bc" and (text[i - 2].isalnum() or text[i - 2] == "_"):
        return False
    j = i + 1
    while j < len(text) and text[j] == "#":
        j += 1
    return j < len(text) and text[j] == '"'


def _char_literal_end(text, i):
    """Index just past a char literal starting at i, or None for a lifetime."""
    if i + 1 < len(text) and text[i + 1] == "\\":
        end = text.find("'", i + 3)
        return None if end == -1 or end - i > 12 else end + 1
    if i + 2 < len(text) and text[i + 2] == "'" and text[i + 1] != "\n":
        return i + 3
    return None


def _normalize(fragment):
    return " ".join(fragment.split())


def _matching_close(code, start, open_char, close_char):
    """Index of the bracket closing the one at `start`, or None."""
    depth = 0
    for k in range(start, len(code)):
        ch = code[k]
        if ch == open_char:
            depth += 1
        elif ch == close_char:
            if close_char == ">" and k > 0 and code[k - 1] == "-":
                continue  # `->` inside a fn type
            depth -= 1
            if depth == 0:
                return k
    return None


def _top_level_args(generics):
    args, depth, current = [], 0, []
    for k, ch in enumerate(generics):
        if ch in "<([{":
            depth += 1
        elif ch in ")]}" or (ch == ">" and not (k > 0 and generics[k - 1] == "-")):
            depth -= 1
        if ch == "," and depth == 0:
            args.append("".join(current))
            current = []
        else:
            current.append(ch)
    args.append("".join(current))
    return [arg.strip() for arg in args if arg.strip()]


def measure(path, text):
    """Counts and findings for one file: ({metric: n}, {metric: [(line, text), ...]}).

    Lines are 1-based. non_test_lines here sees only this file; `scan` also
    zeroes a file its parent declares under a test-only cfg.
    """
    counts, found, _, _ = analyze(path, text)
    return counts, found


def analyze(path, text):
    """measure(), plus the file's out-of-line module declarations and whether it is test code by itself."""
    raw_lines = text.split("\n")
    if raw_lines and raw_lines[-1] == "":
        raw_lines.pop()
    pairs = split_code_and_comments(text)
    code = "\n".join(line_code for line_code, _ in pairs)
    line_starts = [0]
    for line_code, _ in pairs:
        line_starts.append(line_starts[-1] + len(line_code) + 1)

    def line_of(offset):
        lo, hi = 0, len(pairs) - 1
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if line_starts[mid] <= offset:
                lo = mid
            else:
                hi = mid - 1
        return lo

    found = {metric: [] for metric in LINE_FOUND}
    for match in RESULT_OPEN.finditer(code):
        open_at = match.end() - 1
        close = _matching_close(code, open_at, "<", ">")
        if close is None:
            continue
        args = _top_level_args(code[open_at + 1 : close])
        if len(args) == 2 and _normalize(args[1]).replace(" ", "") in STRING_TYPES:
            line = line_of(match.start())
            found["string_errors"].append((line + 1, _normalize(raw_lines[line])))
    for index, (_, comment) in enumerate(pairs):
        if comment and HISTORY.search(comment):
            found["history_comments"].append((index + 1, _normalize(comment)))
    counts = {metric: len(found[metric]) for metric in LINE_FOUND}
    scope, gated_file = scope_of(text)
    counts["non_test_lines"] = non_test_line_count(path, len(raw_lines), scope, gated_file)
    return counts, found, scope.declarations, is_test_path(path) or gated_file


# --- Which lines are test code ---------------------------------------------

Token = collections.namedtuple("Token", "kind text line end_line")
ModDecl = collections.namedtuple("ModDecl", "name path_attr inline_path test_only")


def tokenize(text):
    """Rust tokens with 0-based start and end lines.

    Kinds: ident, punct (one character), literal, lifetime, doc_outer
    (`///`, `/** */`) and doc_inner (`//!`, `/*! */`). Plain comments are
    dropped, so a brace in a comment, string, raw string or char is never punct.
    """
    tokens, i, n, line = [], 0, len(text), 0

    def emit(kind, start, end):
        nonlocal line
        piece = text[start:end]
        tokens.append(Token(kind, piece, line, line + piece.count("\n")))
        line += piece.count("\n")

    while i < n:
        c = text[i]
        if c == "\n":
            line, i = line + 1, i + 1
        elif c.isspace():
            i += 1
        elif text.startswith("//", i):
            end = text.find("\n", i)
            end = n if end == -1 else end
            if text.startswith("///", i) and not text.startswith("////", i):
                emit("doc_outer", i, end)
            elif text.startswith("//!", i):
                emit("doc_inner", i, end)
            i = end
        elif text.startswith("/*", i):
            depth, j = 1, i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth, j = depth + 1, j + 2
                elif text.startswith("*/", j):
                    depth, j = depth - 1, j + 2
                else:
                    j += 1
            piece = text[i:j]
            if piece.startswith("/**") and not piece.startswith("/***") and piece != "/**/":
                emit("doc_outer", i, j)
            elif piece.startswith("/*!"):
                emit("doc_inner", i, j)
            else:
                line += piece.count("\n")
            i = j
        elif c in "bc" and text.startswith("r", i + 1) and _raw_string_start(text, i + 1):
            j = _raw_string_end(text, i + 1)
            emit("literal", i, j)
            i = j
        elif c == "r" and _raw_string_start(text, i):
            j = _raw_string_end(text, i)
            emit("literal", i, j)
            i = j
        elif c == '"' or (c in "bc" and text.startswith('"', i + 1)):
            j = text.index('"', i) + 1
            while j < n and text[j] != '"':
                j += 2 if text[j] == "\\" else 1
            emit("literal", i, min(j + 1, n))
            i = j + 1
        elif c == "'" or (c == "b" and text.startswith("'", i + 1)):
            quote = i if c == "'" else i + 1
            end = _char_literal_end(text, quote)
            if end is None:  # a lifetime or label
                j = quote + 1
                while j < n and (text[j].isalnum() or text[j] == "_"):
                    j += 1
                emit("lifetime", i, j)
                i = j
            else:
                emit("literal", i, end)
                i = end
        elif c.isalpha() or c == "_":
            j = i + 1
            if c == "r" and text.startswith("#", j) and j + 1 < n and (text[j + 1].isalpha() or text[j + 1] == "_"):
                j += 1  # a raw identifier, r#type
            while j < n and (text[j].isalnum() or text[j] == "_"):
                j += 1
            emit("ident", i, j)
            i = j
        elif c.isdigit():
            j = i + 1
            while j < n and (text[j].isalnum() or text[j] == "_" or (text[j] == "." and j + 1 < n and text[j + 1].isdigit())):
                j += 1
            emit("literal", i, j)
            i = j
        else:
            emit("punct", i, i + 1)
            i += 1
    return tokens


def _raw_string_end(text, r_index):
    """Index just past the raw string whose `r` is at r_index."""
    j = r_index + 1
    while text[j] == "#":
        j += 1
    close = '"' + "#" * (j - r_index - 1)
    end = text.find(close, j + 1)
    return len(text) if end == -1 else end + len(close)


OPENERS, CLOSERS = "([{", ")]}"
# Items that end at a `;` even when they hold braces: `const X: T = T { .. };`.
SEMICOLON_ITEMS = {"use", "const", "static", "type", "let", "crate"}
# Items whose braces hold more items, where a test-only item can sit.
ITEM_BODIES = {"mod", "impl", "trait", "extern"}
ITEM_QUALIFIERS = {"unsafe", "async", "default", "auto", "safe"}


def _is(token, kind, text=None):
    return token is not None and token.kind == kind and (text is None or token.text == text)


def _close_of(tokens, start, end):
    """Index of the punct closing the opener at start, or end - 1 when unbalanced."""
    depth = 0
    for k in range(start, end):
        token = tokens[k]
        if token.kind != "punct":
            continue
        if token.text in OPENERS:
            depth += 1
        elif token.text in CLOSERS:
            depth -= 1
            if depth == 0:
                return k
    return end - 1


def _cfg_value(tokens, k, end):
    """Evaluate the cfg predicate at k with `test` off: (False | True | None for unknown, next index)."""
    head = tokens[k] if k < end else None
    if not _is(head, "ident"):
        return None, k + 1
    k += 1
    if k < end and _is(tokens[k], "punct", "="):
        return None, k + 2  # `feature = "x"`: unknown
    if k < end and _is(tokens[k], "punct", "(") and head.text in ("all", "any", "not"):
        close = _close_of(tokens, k, end)
        values, j = [], k + 1
        while j < close:
            value, j = _cfg_value(tokens, j, close)
            values.append(value)
            if j < close and _is(tokens[j], "punct", ","):
                j += 1
        if head.text == "not":
            value = None if not values or values[0] is None else not values[0]
        elif head.text == "all":
            value = False if False in values else (None if None in values else True)
        else:
            value = True if True in values else (None if None in values else False)
        return value, close + 1
    return (False if head.text == "test" else None), k


def is_test_only_cfg(attribute):
    """True when the attribute's tokens (inside `#[...]`) are a `cfg` false whenever `test` is off.

    `cfg(test)`, `cfg(all(test, unix))` and nested `all` qualify; `cfg(any(test,
    feature = "x"))` and `cfg(not(test))` compile into non-test builds, so they
    do not. `cfg_attr(test, ...)` is not a gate.
    """
    if len(attribute) < 3 or not _is(attribute[0], "ident", "cfg") or not _is(attribute[1], "punct", "("):
        return False
    close = _close_of(attribute, 1, len(attribute))
    value, _ = _cfg_value(attribute, 2, close)
    return value is False


def _item_end(tokens, i, end, semicolon_item):
    """(index of the item's last token, index of the `{` opening its body or None)."""
    depth, body = 0, None
    for k in range(i, end):
        token = tokens[k]
        if token.kind != "punct":
            continue
        if token.text in OPENERS:
            if depth == 0 and token.text == "{":
                body = k
            depth += 1
        elif token.text in CLOSERS:
            depth -= 1
            if depth <= 0 and token.text == "}" and not semicolon_item:
                return k, body
            depth = max(depth, 0)
        elif token.text == ";" and depth == 0:
            return k, None
    return end - 1, body


def _item_keyword(tokens, i, end):
    """Index of the token naming the item's kind, past visibility and qualifiers."""
    k = i
    if k < end and _is(tokens[k], "ident", "pub"):
        k += 1
        if k < end and _is(tokens[k], "punct", "("):
            k = _close_of(tokens, k, end) + 1
    while k < end:
        token = tokens[k]
        following = tokens[k + 1] if k + 1 < end else None
        if _is(token, "ident") and token.text in ITEM_QUALIFIERS:
            k += 1
        elif _is(token, "ident", "const") and _is(following, "ident") and following.text in ("fn", "unsafe", "async", "extern"):
            k += 1
        elif _is(token, "ident", "extern") and _is(following, "literal"):
            after = tokens[k + 2] if k + 2 < end else None
            if _is(after, "punct", "{"):
                return k  # an `extern "C" { .. }` block
            k += 2  # `extern "C" fn`
        else:
            return k
    return k


class _Scope:
    """Test-only token ranges and out-of-line module declarations of one file."""

    def __init__(self, tokens):
        self.tokens = tokens
        self.test_ranges = []
        self.declarations = []

    def walk(self, i, end, inline_path=(), in_test=False):
        """Walk the items in tokens[i:end]; True when an inner attribute gates the whole scope."""
        tokens, gated_scope = self.tokens, False
        first_declaration = len(self.declarations)
        while i < end:
            start, test_only, path_attr = i, False, None
            while i < end:
                token = tokens[i]
                if token.kind in ("doc_outer", "doc_inner"):
                    i += 1
                    if token.kind == "doc_inner":
                        start = i  # an inner doc belongs to the scope, not the next item
                    continue
                if not _is(token, "punct", "#"):
                    break
                inner = i + 1 < end and _is(tokens[i + 1], "punct", "!")
                bracket = i + (2 if inner else 1)
                if bracket >= end or not _is(tokens[bracket], "punct", "["):
                    break
                close = _close_of(tokens, bracket, end)
                attribute = tokens[bracket + 1 : close]
                if inner:
                    gated_scope = gated_scope or is_test_only_cfg(attribute)
                    start = close + 1
                else:
                    test_only = test_only or is_test_only_cfg(attribute)
                    if len(attribute) == 3 and _is(attribute[0], "ident", "path") and _is(attribute[2], "literal"):
                        path_attr = attribute[2].text.strip('"')
                i = close + 1
            if i >= end:
                break
            if _is(tokens[i], "punct", ";"):
                i += 1
                continue
            keyword = _item_keyword(tokens, i, end)
            kind = tokens[keyword].text if keyword < end else ""
            if kind == "extern" and keyword + 1 < end and _is(tokens[keyword + 1], "ident", "crate"):
                kind = "crate"
            last, body = _item_end(tokens, i, end, kind in SEMICOLON_ITEMS)
            item_test = in_test or test_only
            if test_only and not in_test:
                self.test_ranges.append((start, last))
            if kind == "mod" and keyword + 1 < end and _is(tokens[keyword + 1], "ident"):
                name = tokens[keyword + 1].text
                if body is None:
                    self.declarations.append(ModDecl(name, path_attr, inline_path, item_test))
                elif self.walk(body + 1, last, inline_path + (path_attr or name,), item_test) and not item_test:
                    self.test_ranges.append((start, last))
            elif kind in ITEM_BODIES and body is not None:
                self.walk(body + 1, last, inline_path, item_test)
            i = last + 1
        if gated_scope:  # `#![cfg(test)]` gates every module declared inside this scope
            self.declarations[first_declaration:] = [
                declaration._replace(test_only=True) for declaration in self.declarations[first_declaration:]
            ]
        return gated_scope


def scope_of(text):
    """(the file's _Scope: tokens, test-only ranges, `mod name;` declarations; whether `#![cfg(test)]` gates it)."""
    scope = _Scope(tokenize(text))
    gated_file = scope.walk(0, len(scope.tokens))
    return scope, gated_file


def is_test_path(path):
    parts = Path(path).parts
    name = parts[-1]
    return "tests" in parts[:-1] or name == "tests.rs" or name.endswith("_tests.rs")


def non_test_line_count(path, line_count, scope, gated_file):
    """Every line, less the lines inside test-only items.

    A line is test code when every token on it belongs to a test-only item, or
    it has no token (blank, a plain comment, inside a multi-line literal) and
    sits within one. Blank and comment lines outside test items count.
    """
    if is_test_path(path) or gated_file:
        return 0
    return line_count - len(test_line_numbers(scope, line_count))


def test_line_numbers(scope, line_count):
    """The 0-based lines non_test_line_count treats as test code."""
    if not scope.test_ranges:
        return set()
    tokens = scope.tokens
    production_line, in_test_item = set(), set()
    in_range = [False] * len(tokens)
    for first, last in scope.test_ranges:
        in_range[first : last + 1] = [True] * (last + 1 - first)
        in_test_item.update(range(tokens[first].line, tokens[last].end_line + 1))
    for index, token in enumerate(tokens):
        if not in_range[index]:
            production_line.update(range(token.line, token.end_line + 1))
    return {line for line in in_test_item if line not in production_line and line < line_count}


def _is_mod_rs(path):
    parts = Path(path).parts
    return parts[-1] in ("lib.rs", "main.rs", "mod.rs", "build.rs") or (len(parts) > 1 and parts[-2] == "bin")


def declared_paths(path, declaration):
    """Candidate repo paths of the file an out-of-line `mod name;` in path loads.

    An enclosing inline module with `#[path = "dir"]` names its directory by that
    path in place of its name, so its inline_path component is the attribute.
    """
    directory = Path(path).parent
    base = directory if _is_mod_rs(path) else directory / Path(path).stem
    nested = base.joinpath(*declaration.inline_path)
    if declaration.path_attr:
        root = directory if not declaration.inline_path else nested
        return [posixpath.normpath((root / declaration.path_attr).as_posix())]
    return [posixpath.normpath((nested / f"{declaration.name}.rs").as_posix()),
            posixpath.normpath((nested / declaration.name / "mod.rs").as_posix())]


def declared_test_files(declarations, test_files):
    """Files only test builds load: every declaration of them is test-only or sits in test code.

    declarations maps each scanned path to its out-of-line module
    declarations; test_files are the paths already known to be test code (by
    name, or `#![cfg(test)]`). A file that some production module also loads
    (a build script's support file a test re-includes, say) is not returned,
    and neither is a file no declaration reaches (a crate root).
    """
    declarers = collections.defaultdict(list)  # target -> [(declaring path, test-only)]
    for path, decls in declarations.items():
        for declaration in decls:
            target = next((c for c in declared_paths(path, declaration) if c in declarations), None)
            if target is not None:
                declarers[target].append((path, declaration.test_only))
    test_code, found = set(test_files), set()
    changed = True
    while changed:
        changed = False
        for target, sources in declarers.items():
            if target in test_code:
                continue
            if all(test_only or source in test_code for source, test_only in sources):
                test_code.add(target)
                found.add(target)
                changed = True
    return found


# --- The checkout ----------------------------------------------------------


def git(root, *args, check=True):
    result = subprocess.run(["git", "-C", str(root), *args], capture_output=True)
    if check and result.returncode != 0:
        raise InputError(f"git {' '.join(args)} failed: {result.stderr.decode(errors='replace').strip()}")
    return result


def tracked_rust_files(root):
    listing = git(root, "ls-files", "-z", "--", "*.rs").stdout.decode()
    paths = []
    for path in listing.split("\0"):
        if not path or Path(path).parts[0] in EXCLUDED_TOP_DIRS:
            continue
        if (Path(root) / path).is_file():  # deleted in the work tree but not yet staged
            paths.append(path)
    return sorted(paths)


def read_text(data):
    return data.decode("utf-8", errors="replace")


def scan(root):
    """{path: (counts, findings)} for every tracked .rs file."""
    shapes, declarations, test_files = {}, {}, set()
    for path in tracked_rust_files(root):
        counts, found, declarations[path], is_test_file = analyze(path, read_text((Path(root) / path).read_bytes()))
        shapes[path] = (counts, found)
        if is_test_file:
            test_files.add(path)
    for path in declared_test_files(declarations, test_files):
        shapes[path][0]["non_test_lines"] = 0
    return shapes


# --- Check -----------------------------------------------------------------

WHAT = {
    "non_test_lines": "non-test lines",
    "string_errors": "Result<_, String> (use a thiserror enum)",
    "history_comments": "history comment (say what the code is now)",
}


def violations(shapes):
    """One message per metric over its limit, with each offending line for the line-found metrics."""
    found = []
    for path, (counts, lines) in sorted(shapes.items()):
        if counts["non_test_lines"] > LIMITS["non_test_lines"]:
            found.append(
                f"{path}: non_test_lines is {counts['non_test_lines']}; the limit is {LIMITS['non_test_lines']}"
                " (split by concept into a module directory)"
            )
        in_file = sorted((line, metric, text) for metric in LINE_FOUND for line, text in lines[metric])
        for line, metric, text in in_file:
            found.append(f"{path}:{line}: {WHAT[metric]}: {text}")
    return found


def run_check(root):
    shapes = scan(root)
    found = violations(shapes)
    if found:
        print(f"code-shape: {len(found)} violation(s)", file=sys.stderr)
        for line in found:
            print("  " + line, file=sys.stderr)
        print("Fix the code (library skill clean-code). There is no baseline to raise.", file=sys.stderr)
        return 1
    largest = max((counts["non_test_lines"], path) for path, (counts, _) in shapes.items()) if shapes else (0, "-")
    print(
        f"code-shape: {len(shapes)} files within limits "
        f"(largest: {largest[1]}, {largest[0]} of {LIMITS['non_test_lines']} non-test lines)"
    )
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=None, help="repository root (default: this script's repo)")
    args = parser.parse_args(argv)
    root = Path(args.root) if args.root else Path(__file__).resolve().parents[1]
    try:
        if git(root, "rev-parse", "--is-inside-work-tree", check=False).stdout.strip() != b"true":
            raise InputError(f"{root} is not a git work tree")
        return run_check(root)
    except InputError as error:
        print(f"code-shape: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
