"""Tests for the parsers and verdicts of suite.py. Run with unittest."""
import json
import os
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import suite

HERE = os.path.dirname(os.path.abspath(__file__))


class TestCpuSpec(unittest.TestCase):
    def test_single_ranges(self):
        self.assertEqual(suite.ncpus("0-3"), 4)

    def test_mixed_spec(self):
        self.assertEqual(suite.ncpus("4-9,21-24"), 10)

    def test_single_core(self):
        self.assertEqual(suite.ncpus("20"), 1)


class TestInterval(unittest.TestCase):
    def test_rate_to_interval(self):
        self.assertEqual(suite.rate_to_interval(20000, 1000), 50)

    def test_minimum_is_one(self):
        self.assertEqual(suite.rate_to_interval(10 ** 9, 10), 1)


class TestDropped(unittest.TestCase):
    def test_sums_drop_keys(self):
        c = {"messages.received": 10, "messages.dropped": 3, "queue.shed": 2}
        self.assertEqual(suite.dropped_total(c), 5)

    def test_empty(self):
        self.assertEqual(suite.dropped_total({}), 0)


class TestDelta(unittest.TestCase):
    def test_nested_path(self):
        a = {"api": {"messages.received": 100}}
        b = {"api": {"messages.received": 300}}
        self.assertEqual(suite.delta(a, b, "api", "messages.received"), 200)

    def test_missing_key_is_zero(self):
        self.assertEqual(suite.delta({}, {"api": {}}, "api", "x"), 0)


class TestGenLatencyParse(unittest.TestCase):
    def test_mean_and_max(self):
        text = ("sub: publish_latency avg=3ms\n"
                "sub: publish_latency avg=7ms\n")
        avg, mx = suite.gen_latency_from_text(text)
        self.assertEqual(avg, 5.0)
        self.assertEqual(mx, 7)

    def test_no_lines(self):
        self.assertEqual(suite.gen_latency_from_text("no data"), (None, None))


class TestProbeParse(unittest.TestCase):
    def test_splits_intervals_and_final(self):
        text = ('{"type":"interval","t_s":10,"count":500,"p99_us":4000}\n'
                '{"type":"final","count":9000,"p99_us":8000}\n'
                "not json\n")
        intervals, final = suite.parse_probe_lines(text)
        self.assertEqual(len(intervals), 1)
        self.assertEqual(final["p99_us"], 8000)

    def test_empty(self):
        self.assertEqual(suite.parse_probe_lines(""), ([], None))


class TestSyscountRead(unittest.TestCase):
    def test_sums_stripes(self):
        vals = [0] * 32
        vals[0] = 5
        vals[16] = 7
        vals[1] = 100
        vals[17] = 50
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(struct.pack("<32Q", *vals))
            path = f.name
        try:
            out = suite.syscount_read(path)
            self.assertEqual(out["send"], 12)
            self.assertEqual(out["send_bytes"], 150)
        finally:
            os.unlink(path)

    def test_missing_file_is_none(self):
        self.assertIsNone(suite.syscount_read("/nonexistent-bench-file.bin"))


class TestVerdicts(unittest.TestCase):
    def test_good_step_passes(self):
        row = {"delivery_ratio": 1.0, "broker_alive": True,
               "generator_limited": False, "mem_first_mib": 100.0,
               "mem_mib": 102.0, "drain_secs": 2.0}
        verdict, failed = suite.evaluate_step(row)
        self.assertEqual(verdict, "pass")
        self.assertEqual(failed, [])

    def test_low_delivery_fails(self):
        row = {"delivery_ratio": 0.5, "broker_alive": True,
               "generator_limited": False, "mem_first_mib": 100.0,
               "mem_mib": 102.0, "drain_secs": 2.0}
        verdict, failed = suite.evaluate_step(row)
        self.assertEqual(verdict, "fail")
        self.assertIn("delivery-below-90pct", failed)

    def test_dead_broker_fails(self):
        row = {"delivery_ratio": 1.0, "broker_alive": False,
               "generator_limited": False, "mem_first_mib": 100.0,
               "mem_mib": 102.0, "drain_secs": 2.0}
        self.assertTrue(suite.is_failing(row))

    def test_rising_memory_fails(self):
        row = {"delivery_ratio": 1.0, "broker_alive": True,
               "generator_limited": False, "mem_first_mib": 100.0,
               "mem_mib": 500.0, "drain_secs": 2.0}
        _, failed = suite.evaluate_step(row)
        self.assertIn("memory-rising", failed)

    def test_latency_series(self):
        flat = [5.0] * 9
        self.assertTrue(suite.latency_flat_from_series(flat))
        rising = [5.0] * 6 + [500.0] * 3
        self.assertFalse(suite.latency_flat_from_series(rising))
        self.assertIsNone(suite.latency_flat_from_series([]))


class TestRecommended(unittest.TestCase):
    def test_picks_cpu_nearest_75(self):
        rows = [{"step": "a", "cpu_pct_of_4": 40.0, "delivery_ratio": 1.0},
                {"step": "b", "cpu_pct_of_4": 74.0, "delivery_ratio": 1.0},
                {"step": "c", "cpu_pct_of_4": 95.0, "delivery_ratio": 1.0}]
        self.assertEqual(suite.pick_recommended(rows)["step"], "b")

    def test_skips_limited_steps(self):
        rows = [{"step": "a", "cpu_pct_of_4": 75.0, "delivery_ratio": 1.0,
                 "generator_limited": True},
                {"step": "b", "cpu_pct_of_4": 60.0, "delivery_ratio": 1.0}]
        self.assertEqual(suite.pick_recommended(rows)["step"], "b")

    def test_empty_is_none(self):
        self.assertIsNone(suite.pick_recommended([]))


class TestFixture(unittest.TestCase):
    def test_fixture_loads(self):
        path = os.path.join(HERE, "fixtures", "sample.jsonl")
        rows = suite.read_rows(path)
        self.assertEqual(len(rows), 5)
        names = {r["scenario"] for r in rows}
        self.assertEqual(names, {"s1-qos1", "s3-fanout"})

    def test_scenarios_exist(self):
        for name in ("s1-qos0", "s1-qos1", "s1-qos2", "s2-payload",
                     "s3-fanin", "s3-fanout"):
            self.assertIn(name, suite.SCENARIOS)

    def test_compare_runs(self):
        with tempfile.TemporaryDirectory() as newd, tempfile.TemporaryDirectory() as oldd:
            s = {"scenario": "s1-qos1", "recommended_out_rate": 20000}
            for d in (newd, oldd):
                with open(os.path.join(d, "s1-qos1.summary.json"), "w") as f:
                    json.dump(s, f)
            rows = suite.compare_runs(newd, oldd)
            self.assertEqual(rows[0]["change_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
