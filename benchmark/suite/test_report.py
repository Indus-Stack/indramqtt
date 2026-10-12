"""Tests for report.py with a small fixture. Run with unittest."""
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import report

HERE = os.path.dirname(os.path.abspath(__file__))
FIXTURE = os.path.join(HERE, "fixtures", "sample.jsonl")


class TestReport(unittest.TestCase):
    def test_page_has_scenarios_and_svg(self):
        rows = report.read_rows(FIXTURE)
        by_name = {}
        for r in rows:
            by_name.setdefault(r["scenario"], []).append(r)
        env = {"cpu_model": "Test CPU Model 1", "cpu_cores": 24,
               "memory_mib": 8192.0, "kernel": "6.8.0-test",
               "broker_cores": 4, "broker_memory": "8g",
               "build_sha256": "abc123"}
        page = report.make_page(by_name, {}, env)
        self.assertIn("<html", page)
        self.assertIn("s1-qos1", page)
        self.assertIn("s3-fanout", page)
        self.assertIn("<svg", page)
        self.assertIn("Recommended load", page)
        self.assertIn("Test CPU Model 1", page)

    def test_recommended_values_in_table(self):
        rows = report.read_rows(FIXTURE)
        s1 = [r for r in rows if r["scenario"] == "s1-qos1"]
        rec = report.pick_recommended(s1)
        self.assertEqual(rec["step"], "r20000")
        page = report.make_page({"s1-qos1": s1}, {}, {})
        self.assertIn("r20000", page)
        self.assertIn("19800", page)

    def test_no_external_network(self):
        rows = report.read_rows(FIXTURE)
        page = report.make_page({"s1-qos1": rows[:1]}, {}, {})
        low = page.lower()
        self.assertNotIn("http://", low)
        self.assertNotIn("https://", low)
        self.assertNotIn("<script", low)

    def test_missing_values_show_plainly(self):
        rows = [{"scenario": "s1-qos1", "step": "r1", "offered": 100,
                 "in_rate": 100, "out_rate": 100, "cpu_pct_of_4": 10.0,
                 "delivery_ratio": 1.0, "mem_mib": 100.0}]
        page = report.make_page({"s1-qos1": rows}, {}, {})
        self.assertIn("not measured", page)

    def test_fixture_makes_valid_page(self):
        with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as f:
            dest = f.name
        try:
            self.assertEqual(report.main(["report.py", FIXTURE, dest]), 0)
            with open(dest) as f:
                page = f.read()
            self.assertIn("</html>", page)
            self.assertIn("<svg", page)
        finally:
            os.unlink(dest)

    def test_directory_load(self):
        fixdir = os.path.join(HERE, "fixtures")
        scenarios, _, env = report.load_run(fixdir)
        self.assertIn("s1-qos1", scenarios)
        self.assertIn("s3-fanout", scenarios)
        self.assertEqual(env.get("cpu_model"), "Test CPU Model 1")


if __name__ == "__main__":
    unittest.main()
