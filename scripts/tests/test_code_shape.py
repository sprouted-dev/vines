"""Tests for scripts/code-shape.py: the real CLI against throwaway git repos."""

import importlib.util
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts/code-shape.py"
spec = importlib.util.spec_from_file_location("shape", SCRIPT)
shape = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shape)

CLEAN = "pub fn add(a: u32, b: u32) -> u32 {\n    a + b\n}\n"
STRING_ERROR = "pub fn parse(raw: &str) -> Result<u32, String> {\n    raw.parse().map_err(|_| raw.to_owned())\n}\n"


def lines(n, prefix="pub const X"):
    return "".join(f"{prefix}{i}: u32 = {i};\n" for i in range(n))


class Repo:
    """A throwaway git repo, driven through the CLI."""

    def __init__(self, test):
        self._dir = tempfile.TemporaryDirectory()
        test.addCleanup(self._dir.cleanup)
        self.root = Path(self._dir.name)
        config = self.root / ".gitconfig-test"
        config.write_text("")
        self.env = {
            **os.environ,
            "GIT_CONFIG_GLOBAL": str(config),
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_AUTHOR_NAME": "t",
            "GIT_AUTHOR_EMAIL": "t@example.invalid",
            "GIT_COMMITTER_NAME": "t",
            "GIT_COMMITTER_EMAIL": "t@example.invalid",
        }
        self.git("init", "-q", "-b", "main")

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, env=self.env, check=True, capture_output=True, text=True)

    def write(self, path, text, add=True):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)
        if add:
            self.git("add", "-f", path)

    def run(self, *args):
        return subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.root), *args],
            capture_output=True, text=True, env=self.env,
        )


class CheckTests(unittest.TestCase):
    def setUp(self):
        self.repo = Repo(self)

    def test_a_file_over_the_line_limit_fails_naming_file_count_and_limit(self):
        self.repo.write("src/big.rs", lines(801))
        result = self.repo.run()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("src/big.rs: non_test_lines is 801; the limit is 800", result.stderr)

    def test_a_file_at_the_limit_with_tests_below_passes(self):
        self.repo.write("src/edge.rs", lines(800) + "#[cfg(test)]\nmod tests {\n" + lines(400, "    const T") + "}\n")
        result = self.repo.run()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("1 files within limits (largest: src/edge.rs, 800 of 800 non-test lines)", result.stdout)

    def test_a_string_error_fails_with_its_line(self):
        self.repo.write("src/lib.rs", CLEAN + STRING_ERROR)
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        self.assertIn(
            "src/lib.rs:4: Result<_, String> (use a thiserror enum): "
            "pub fn parse(raw: &str) -> Result<u32, String> {",
            result.stderr,
        )

    def test_a_history_comment_fails_with_its_line(self):
        self.repo.write("src/c.rs", "fn f() {}\n// Formerly the parser.\n")
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        self.assertIn("src/c.rs:2: history comment (say what the code is now): // Formerly the parser.", result.stderr)

    def test_debt_in_test_files_counts_but_their_length_does_not(self):
        self.repo.write("tests/it.rs", lines(2000) + STRING_ERROR)
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        self.assertIn("tests/it.rs:2001: Result<_, String>", result.stderr)
        self.assertNotIn("non_test_lines", result.stderr)

    def test_every_violation_is_reported_not_just_the_first(self):
        self.repo.write("src/a.rs", STRING_ERROR + STRING_ERROR.replace("parse", "parse_two"))
        self.repo.write("src/b.rs", lines(900) + "// This used to live in a.rs.\n")
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        self.assertIn("code-shape: 4 violation(s)", result.stderr)
        self.assertIn("src/a.rs:1:", result.stderr)
        self.assertIn("src/a.rs:4:", result.stderr)
        self.assertIn("src/b.rs: non_test_lines is 901", result.stderr)
        self.assertIn("src/b.rs:901: history comment", result.stderr)

    def test_findings_in_a_file_are_reported_in_line_order(self):
        self.repo.write("src/lib.rs", "// Formerly x.\n" + STRING_ERROR + "// Previously y.\n")
        result = self.repo.run()
        reported = [line.split(":")[1] for line in result.stderr.splitlines() if line.startswith("  src/lib.rs:")]
        self.assertEqual(reported, ["1", "2", "5"])

    def test_an_unreasoned_allow_is_left_to_clippy(self):
        self.repo.write("src/lib.rs", "#[allow(dead_code)]\nfn unused() {}\n")
        self.assertEqual(self.repo.run().returncode, 0)

    def test_deleted_from_work_tree_but_still_in_index_is_ignored(self):
        self.repo.write("src/gone.rs", lines(900))
        (self.repo.root / "src/gone.rs").unlink()
        result = self.repo.run()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("0 files within limits", result.stdout)

    def test_only_tracked_files_outside_trees_and_target_are_scanned(self):
        self.repo.write("src/untracked.rs", STRING_ERROR, add=False)
        self.repo.write(".trees/feature/x/src/lib.rs", STRING_ERROR)
        self.repo.write("target/debug/build/out.rs", STRING_ERROR)
        self.repo.write("src/notes.txt", STRING_ERROR)
        result = self.repo.run()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("0 files within limits", result.stdout)

    def test_non_utf8_bytes_do_not_crash_the_scan(self):
        target = self.repo.root / "src/bytes.rs"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"// caf\xe9\nfn f() {}\n")
        self.repo.git("add", "src/bytes.rs")
        self.assertEqual(self.repo.run().returncode, 0)

    def test_not_a_git_checkout_is_bad_input(self):
        with tempfile.TemporaryDirectory() as plain:
            result = subprocess.run([sys.executable, str(SCRIPT), "--root", plain], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("not a git work tree", result.stderr)

    def test_a_missing_root_is_bad_input(self):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), "--root", str(self.repo.root / "nowhere")], capture_output=True, text=True
        )
        self.assertEqual(result.returncode, 2)
        self.assertIn("not a git work tree", result.stderr)

    def test_a_failing_git_listing_is_bad_input(self):
        # A broken index makes `git ls-files` fail after rev-parse succeeds.
        self.repo.write("src/lib.rs", CLEAN)
        (self.repo.root / ".git/index").write_bytes(b"not an index")
        result = self.repo.run()
        self.assertEqual(result.returncode, 2)
        self.assertIn("git ls-files", result.stderr)

    def test_an_unknown_argument_is_a_usage_error(self):
        result = self.repo.run("--bogus")
        self.assertEqual(result.returncode, 2)
        self.assertIn("unrecognized arguments: --bogus", result.stderr)


TEST_MOD = "#[cfg(test)]\nmod tests {\n    #[test]\n    fn t() {}\n}\n"


def gated(predicate, body="    fn f() {}\n"):
    return f"#[cfg({predicate})]\nmod gated {{\n{body}}}\n"


class NonTestLinesTests(unittest.TestCase):
    """non_test_lines: every line, less the lines inside test-only items."""

    def count(self, text, path="src/lib.rs"):
        return shape.measure(path, text)[0]["non_test_lines"]

    def test_a_trailing_test_module_with_its_doc_and_attributes_is_not_counted(self):
        text = lines(10) + "\n/// Unit tests.\n#[cfg(test)]\n#[allow(unused, reason = \"x\")]\nmod tests {\n\n    fn t() {}\n}\n"
        self.assertEqual(self.count(text), 11)  # the blank line before the doc comment is outside the item

    def test_production_code_after_a_mid_file_test_module_is_counted(self):
        self.assertEqual(self.count(lines(5) + TEST_MOD + lines(7, "pub const Y")), 12)

    def test_predicates_that_require_test_gate_the_item(self):
        for predicate in (
            "test",
            " test ",
            "all(test, unix)",
            "all(unix, test)",
            "all(test, any(target_os = \"linux\", target_os = \"macos\"))",
            "all(unix, all(test, feature = \"memgraph\"))",
            "all(test, not(windows))",
            "any(test, all(test, unix))",
            "not(not(test))",
        ):
            with self.subTest(predicate):
                self.assertEqual(self.count(lines(3) + gated(predicate) + lines(2, "pub const Y")), 5)

    def test_spacing_inside_the_attribute_does_not_hide_the_gate(self):
        text = "# [ cfg ( all ( test , unix ) ) ]\nmod gated {\n}\n" + lines(2)
        self.assertEqual(self.count(text), 2)

    def test_predicates_that_compile_without_test_are_production(self):
        for predicate in (
            "any(test, feature = \"test-util\")",
            "any(windows, test)",
            "not(test)",
            "not(all(test, unix))",
            "all(unix, any(test, feature = \"x\"))",
            "feature = \"test\"",
            "doctest",
            "unix",
        ):
            with self.subTest(predicate):
                self.assertEqual(self.count(lines(3) + gated(predicate) + lines(2, "pub const Y")), 9)

    def test_cfg_attr_with_test_is_not_a_gate(self):
        text = "#[cfg_attr(test, derive(Debug))]\npub struct S {\n    a: u8,\n}\n" + lines(2)
        self.assertEqual(self.count(text), 6)

    def test_braces_in_literals_and_comments_inside_a_test_module_do_not_move_its_end(self):
        test_module = (
            "#[cfg(test)]\n"
            "mod tests {\n"
            "    const A: &str = \"}\";\n"
            "    const B: &str = \"{{\";\n"
            "    const C: &str = r#\"}\"#;\n"
            "    const D: &str = r##\"{ \"# }\"##;\n"
            "    const E: char = '}';\n"
            "    const F: char = '{';\n"
            "    const G: &[u8] = b\"}\";\n"
            "    const H: &str = \"\\\"}\";\n"
            "    const I: u8 = b'{';\n"
            "    // }\n"
            "    /* { /* } */ { */\n"
            "    /// A doc comment with a brace }\n"
            "    fn f<'a>(x: &'a str) -> &'a str { x }\n"
            "    const J: &str = \"a string\n"
            "}\n"
            "spanning lines {\";\n"
            "}\n"
        )
        self.assertEqual(self.count(lines(2) + test_module + lines(4, "pub const Y")), 6)

    def test_test_items_nested_in_production_modules_are_not_counted(self):
        text = (
            "pub mod outer {\n"
            "    pub fn a() {}\n"
            "    #[cfg(test)]\n"
            "    mod inner {\n"
            "        fn t() {}\n"
            "    }\n"
            "    pub fn b() {}\n"
            "    pub mod deeper {\n"
            "        #[cfg(all(test, unix))]\n"
            "        pub fn only_in_tests() {}\n"
            "        pub fn c() {}\n"
            "    }\n"
            "}\n"
            "#[cfg(test)]\n"
            "mod tests {\n"
            "    mod nested {\n"
            "        #[cfg(not(test))]\n"
            "        fn never() {}\n"
            "    }\n"
            "}\n"
        )
        self.assertEqual(self.count(text), 7)

    def test_inner_cfg_test_gates_its_file_or_module(self):
        self.assertEqual(self.count("//! Helpers for tests.\n#![cfg(test)]\n" + lines(10)), 0)
        self.assertEqual(self.count("mod helpers {\n    #![cfg(test)]\n    fn h() {}\n}\n" + lines(2)), 2)
        self.assertEqual(self.count("#![cfg(any(test, feature = \"x\"))]\n" + lines(2)), 3)

    def test_out_of_line_test_module_declarations_are_not_counted(self):
        text = (
            "/// Tests live in their own file.\n"
            "#[cfg(test)]\n"
            "#[path = \"lib_tests.rs\"]\n"
            "mod tests;\n"
            + lines(3)
            + "mod outer {\n    #[cfg(test)]\n    mod tests;\n}\n"
        )
        self.assertEqual(self.count(text), 5)

    def test_gated_items_other_than_modules_are_not_counted(self):
        text = (
            "#[cfg(test)]\n"
            "use std::collections::HashMap;\n"
            "#[cfg(test)]\n"
            "fn helper() -> [u8; 2] {\n"
            "    [1, 2]\n"
            "}\n"
            "#[cfg(test)]\n"
            "const X: Wrapper = Wrapper { a: 1 };\n"
            "#[cfg(test)]\n"
            "struct S(u8);\n"
            "#[cfg(test)]\n"
            "macro_rules! m {\n"
            "    () => {};\n"
            "}\n"
            "#[cfg(test)]\n"
            "pub(crate) static TABLE: &[&str] = &[\"{\"];\n"
            "pub struct Foo;\n"
            "impl Foo {\n"
            "    pub fn a(&self) {}\n"
            "    #[cfg(test)]\n"
            "    pub fn for_tests(&self) -> u8 {\n"
            "        1\n"
            "    }\n"
            "    pub fn b(&self) {}\n"
            "}\n"
            "#[cfg(test)]\n"
            "impl Foo {\n"
            "    fn more(&self) {}\n"
            "}\n"
            "pub trait T {\n"
            "    #[cfg(test)]\n"
            "    fn probe(&self);\n"
            "}\n"
            + lines(2)
        )
        self.assertEqual(self.count(text), 9)

    def test_blank_and_comment_lines_count_unless_inside_a_test_item(self):
        text = (
            "// A production comment.\n"
            "\n"
            "pub fn a() {}\n"
            "\n"
            "// A plain comment above the tests stays production.\n"
            "#[cfg(test)]\n"
            "mod tests {\n"
            "\n"
            "    // inside\n"
            "}\n"
            "\n"
            "// A trailing comment.\n"
        )
        self.assertEqual(self.count(text), 7)

    def test_a_line_shared_with_production_code_counts_as_production(self):
        text = "#[cfg(test)] fn t() {} pub fn p() {}\npub fn q() {} #[cfg(test)] fn t2() {}\n#[cfg(test)] fn t3() {}\n"
        self.assertEqual(self.count(text), 2)

    def test_unbalanced_input_does_not_crash(self):
        self.assertEqual(self.count(lines(2) + "#[cfg(test)]\nmod tests {\n    fn t() {\n"), 2)
        self.assertEqual(self.count("}\n" + lines(2) + ")\n"), 4)
        self.assertEqual(self.count(lines(2) + "#[cfg(test)]\n"), 3)  # an attribute with no item gates nothing

    def test_test_file_names_count_no_non_test_lines(self):
        for path in ("crate/tests/it.rs", "crate/tests/common/mod.rs", "src/foo/tests.rs", "src/foo_tests.rs"):
            with self.subTest(path):
                self.assertEqual(self.count(lines(3000), path), 0)
        for path in ("src/testsuite.rs", "src/foo_test.rs", "src/soil_test.rs", "src/test_support.rs"):
            with self.subTest(path):
                self.assertEqual(self.count(lines(3), path), 3)


class DeclaredTestFileTests(unittest.TestCase):
    """A file its parent declares under a test-only cfg counts no non-test lines."""

    def setUp(self):
        self.repo = Repo(self)

    def test_files_declared_under_a_test_only_cfg_and_their_children_count_zero(self):
        self.repo.write("src/lib.rs", (
            "#[cfg(test)]\nmod helpers_test;\n"
            "#[cfg(all(test, unix))]\nmod unix_probe;\n"
            "#[cfg(any(test, feature = \"x\"))]\nmod fixtures;\n"
            "mod soil_test;\n"
            "#[cfg(test)]\n#[path = \"elsewhere/odd.rs\"]\nmod odd;\n"
            "mod outer {\n    #[cfg(test)]\n    mod deep;\n}\n"
            "mod net;\nmod runtime;\n"
            "#[cfg(test)]\nmod kit;\n"
            "mod helpers {\n    #![cfg(test)]\n    mod child;\n    mod inline {\n        mod grandchild;\n    }\n}\n"
        ))
        self.repo.write("src/runtime.rs", "#[cfg(test)]\nmod creds_test;\n")
        self.repo.write("src/net/mod.rs", "#[cfg(test)]\nmod probe_test;\n")
        self.repo.write("src/helpers_test.rs", "mod child;\n" + lines(801))
        for path in (
            "src/unix_probe.rs", "src/fixtures.rs", "src/soil_test.rs", "src/elsewhere/odd.rs",
            "src/outer/deep.rs", "src/runtime/creds_test.rs", "src/net/probe_test.rs",
            "src/helpers_test/child.rs", "src/kit/mod.rs", "src/helpers/child.rs", "src/helpers/inline/grandchild.rs",
        ):
            self.repo.write(path, lines(801))
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        over = sorted(line.split(":")[0].strip() for line in result.stderr.splitlines() if "non_test_lines is" in line)
        self.assertEqual(over, ["src/fixtures.rs", "src/soil_test.rs"])

    def test_an_inline_module_path_attribute_names_its_childrens_directory(self):
        self.repo.write("src/lib.rs", (
            "#[path = \"alternate\"]\nmod outer {\n    #[cfg(test)]\n    mod helper;\n    mod live;\n}\n"
            "mod plain;\n"
        ))
        self.repo.write("src/plain.rs", "#[path = \"../shifted\"]\nmod inner {\n    #[cfg(test)]\n    mod kit;\n}\n")
        for path in ("src/alternate/helper.rs", "src/alternate/live.rs", "src/shifted/kit.rs"):
            self.repo.write(path, lines(801))
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        over = sorted(line.split(":")[0].strip() for line in result.stderr.splitlines() if "non_test_lines is" in line)
        self.assertEqual(over, ["src/alternate/live.rs"])

    def test_a_file_production_code_also_loads_is_counted(self):
        self.repo.write("build.rs", "#[path = \"build_support/runtime.rs\"]\nmod runtime_support;\nfn main() {}\n")
        self.repo.write("src/lib.rs", (
            "#[cfg(test)]\n#[path = \"../build_support/runtime.rs\"]\nmod runtime_support_tests;\n"
            "pub mod shared;\n"
        ))
        self.repo.write("tests/it.rs", "#[path = \"../src/shared.rs\"]\nmod shared;\n")
        self.repo.write("build_support/runtime.rs", lines(801))
        self.repo.write("src/shared.rs", lines(801))
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        over = sorted(line.split(":")[0].strip() for line in result.stderr.splitlines() if "non_test_lines is" in line)
        self.assertEqual(over, ["build_support/runtime.rs", "src/shared.rs"])

    def test_extracting_a_mid_file_test_module_leaves_the_count_unchanged(self):
        self.repo.write("src/runtime.rs", lines(700) + TEST_MOD + lines(100, "pub const Y"))
        self.assertEqual(self.repo.run().returncode, 0)
        self.repo.write("src/runtime.rs", lines(700) + "#[cfg(test)]\nmod tests;\n" + lines(101, "pub const Y"))
        self.repo.write("src/runtime/tests.rs", "#[test]\nfn t() {}\n")
        result = self.repo.run()
        self.assertEqual(result.returncode, 1)
        self.assertIn("src/runtime.rs: non_test_lines is 801", result.stderr)


class MeasureTests(unittest.TestCase):
    def counts(self, text, path="src/lib.rs"):
        return shape.measure(path, text)[0]

    def test_string_errors_shapes(self):
        counted = [
            "fn a() -> Result<(), String>;",
            "fn a() -> Result<Box<dyn Fn() -> u8>, String>;",
            "fn a() -> Result<Vec<String>, std::string::String>;",
            "fn a() -> Result<HashMap<String, u8>, String>;",
            "let r = Result::<u8, String>::Ok(1);",
            "fn a() -> Result<\n    u8,\n    String,\n>;",
        ]
        ignored = [
            "fn a() -> io::Result<String>;",
            "fn a() -> Result<String, Error>;",
            "fn a() -> Result<HashMap<String, String>>;",
            "fn a() -> MyResult<u8, String>;",
            'const S: &str = "Result<u8, String>";',
            "// returns Result<u8, String>",
            "/* Result<u8, String> */",
            'const R: &str = r#"Result<u8, String>"#;',
        ]
        for text in counted:
            with self.subTest(text):
                self.assertEqual(self.counts(text)["string_errors"], 1)
        for text in ignored:
            with self.subTest(text):
                self.assertEqual(self.counts(text)["string_errors"], 0)
        self.assertEqual(self.counts("fn a() -> Result<Result<u8, String>, String>;")["string_errors"], 2)

    def test_history_comments(self):
        counted = [
            "// This used to live in service.rs.",
            "/// No longer needed after the split.",
            "//! Moved to garden.rs.",
            "let x = 1; // previously two",
            "/* formerly the router */",
            "/*\n * The parser now lives in kit.\n */",
            "// Before the split this was one file.",
            "// The retired RPC path.",
        ]
        ignored = [
            "// This helper is used to parse ids.",
            "// Locks are used to serialize writers.",
            "// The tool can be used to probe.",
            'let s = "no longer"; // present tense',
            "fn moved_to() {}",
        ]
        for text in counted:
            with self.subTest(text):
                self.assertEqual(self.counts(text)["history_comments"], 1)
        for text in ignored:
            with self.subTest(text):
                self.assertEqual(self.counts(text)["history_comments"], 0)
        self.assertEqual(self.counts("// used to\n// no longer\n")["history_comments"], 2)
        self.assertEqual(self.counts("// used to and no longer\n")["history_comments"], 1)

    def test_lexer_handles_lifetimes_chars_and_nested_comments(self):
        text = (
            "fn f<'a>(s: &'a str) -> Result<char, String> { let q = '\"'; let b = b'\\''; Ok('x') }\n"
            "/* outer /* inner */ still comment Result<u8, String> */\n"
            'const U: &str = "\\"// not a comment";\n'
            "fn g() -> Result<(), String>;\n"
        )
        self.assertEqual(self.counts(text)["string_errors"], 2)


if __name__ == "__main__":
    unittest.main()
