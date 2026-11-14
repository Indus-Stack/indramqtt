#!/usr/bin/env python3
"""Tests for tools/check_site_links.py. Run: python3 tools/test_check_site_links.py -v"""
import contextlib
import io
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import check_site_links  # noqa: E402


def write(root, relative, text):
    path = os.path.join(root, relative)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(text)


class CheckSiteLinks(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dist = self.tmp.name
        write(self.dist, "favicon.svg", "<svg/>")
        write(self.dist, "style.css", "")
        write(
            self.dist,
            "index.html",
            '<html><head><link rel="icon" href="/favicon.svg">'
            '<link rel="stylesheet" href="/style.css"></head><body>'
            '<a href="/rules/">rules</a> <a href="rules/sql/">sql</a> <a href="#top-section">same</a>'
            '<h2 id="top-section">x</h2></body></html>',
        )
        write(self.dist, "rules/index.html", '<h1 id="rules">Rules</h1><a href="sql/">sql</a><a href="../">home</a>')
        write(self.dist, "rules/sql/index.html", '<h2 id="where">Where</h2>')

    def tearDown(self):
        self.tmp.cleanup()

    def run_check(self):
        return check_site_links.check(self.dist)

    def test_a_site_with_working_links_is_clean(self):
        pages, checked, broken = self.run_check()
        self.assertEqual(broken, [])
        self.assertEqual(pages, 3)
        self.assertGreaterEqual(checked, 7)

    def test_a_missing_page_is_broken(self):
        write(self.dist, "rules/index.html", '<a href="./sql.md">sql</a>')
        _, _, broken = self.run_check()
        self.assertEqual([(p, h) for p, h, _ in broken], [("/rules/", "./sql.md")])

    def test_a_missing_anchor_is_broken(self):
        write(self.dist, "rules/index.html", '<a href="sql/#nope">x</a><a href="sql/#where">y</a>')
        _, _, broken = self.run_check()
        self.assertEqual([h for _, h, _ in broken], ["sql/#nope"])

    def test_a_missing_asset_is_broken(self):
        write(self.dist, "rules/index.html", '<link rel="icon" href="/missing.svg"><img src="/nope.png">')
        _, _, broken = self.run_check()
        self.assertEqual(sorted(h for _, h, _ in broken), ["/missing.svg", "/nope.png"])

    def test_external_and_host_paths_are_skipped(self):
        write(
            self.dist,
            "rules/index.html",
            '<a href="https://example.com/x">e</a><a href="mailto:a@b.c">m</a>'
            '<script src="/_vercel/insights/script.js"></script>',
        )
        _, _, broken = self.run_check()
        self.assertEqual(broken, [])

    def test_exit_codes_and_report(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(check_site_links.main([self.dist]), 0)
        write(self.dist, "rules/index.html", '<a href="./sql.md">sql</a>')
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(check_site_links.main([self.dist]), 1)
        self.assertIn("/rules/  ->  ./sql.md", out.getvalue())
        with contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(check_site_links.main([os.path.join(self.dist, "none")]), 2)


if __name__ == "__main__":
    unittest.main()
