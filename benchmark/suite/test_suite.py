"""Tests for the parsers and verdicts of suite.py. Run with unittest."""
import json
import os
import signal
import struct
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

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


EXAMPLE_PERMITTED = "0,4-5,7-8,12,15,17,22,25-27,32,35-36,39,41,45-46,49,54,56,59,62"


class TestCpuLayout(unittest.TestCase):
    """Check CPU assignment from the effective set."""

    def test_parse_example_list(self):
        cores = suite.parse_cpu_list(EXAMPLE_PERMITTED)
        self.assertEqual(len(cores), 24)
        self.assertEqual(cores[:4], [0, 4, 5, 7])

    def test_format_roundtrip(self):
        cores = suite.parse_cpu_list(EXAMPLE_PERMITTED)
        self.assertEqual(suite.parse_cpu_list(suite.format_cpu_list(cores)), cores)

    def test_build_assigns_roles(self):
        cores = suite.parse_cpu_list(EXAMPLE_PERMITTED)
        layout = suite.build_cpu_layout(cores)
        broker = suite.parse_cpu_list(layout["broker"])
        probe = suite.parse_cpu_list(layout["probe"])
        pub = suite.parse_cpu_list(layout["pub"])
        sub = suite.parse_cpu_list(layout["sub"])
        self.assertEqual(broker, cores[:4])
        self.assertEqual(probe, cores[4:5])
        self.assertEqual(len(pub), 9)
        self.assertEqual(len(sub), 10)
        self.assertEqual(sorted(broker + probe + pub + sub), cores)

    def test_env_override_wins(self):
        cores = suite.parse_cpu_list(EXAMPLE_PERMITTED)
        with mock.patch.object(suite, "read_effective_cpus", return_value=cores):
            with mock.patch.dict(os.environ, {"BENCH_BROKER_CPUS": "0-1"}):
                out = suite.resolve_cpus()
        self.assertEqual(out["broker"], "0-1")

    def test_read_prefers_cgroup_file(self):
        with mock.patch.object(suite, "read_file", return_value="0-3,8-11\n"):
            self.assertEqual(suite.read_effective_cpus(), [0, 1, 2, 3, 8, 9, 10, 11])

    def test_read_falls_back_to_affinity(self):
        with mock.patch.object(suite, "read_file", return_value=""):
            with mock.patch.object(os, "sched_getaffinity", return_value=set(range(10))):
                self.assertEqual(suite.read_effective_cpus(), list(range(10)))

    def test_fewer_than_ten_stops(self):
        with self.assertRaises(SystemExit) as cm:
            suite.build_cpu_layout(list(range(9)))
        self.assertIn("10", str(cm.exception))
        with mock.patch.object(suite, "read_effective_cpus", return_value=list(range(5))):
            with self.assertRaises(SystemExit):
                suite.resolve_cpus()


class FakeCompleted:
    """Fake result of subprocess.run."""

    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr


class TestDockerRunChecked(unittest.TestCase):
    """Check that a failed docker run raises with stderr."""

    def test_failure_raises_with_stderr(self):
        msg = "Requested CPUs are not available"
        with mock.patch.object(suite.subprocess, "run",
                               return_value=FakeCompleted(1, "", msg)):
            with self.assertRaises(RuntimeError) as cm:
                suite.docker_run("docker", "run", "x")
        self.assertIn(msg, str(cm.exception))

    def test_success_returns_stdout(self):
        with mock.patch.object(suite.subprocess, "run",
                               return_value=FakeCompleted(0, "cid123\n", "")):
            self.assertEqual(suite.docker_run("docker", "run", "x"), "cid123\n")

    def test_generator_uses_checked_run(self):
        calls = {}

        def fake_run(*args):
            calls["args"] = list(args)
            return "ok"

        with mock.patch.object(suite, "docker_run", side_effect=fake_run):
            with mock.patch.object(suite, "sh", return_value="cid"):
                suite.start_generator("bs-t", "0-3", ["loadgen sub"], "img:1")
        self.assertEqual(calls["args"][:2], ["docker", "run"])
        joined = " ".join(calls["args"])
        self.assertIn("+S", joined)
        self.assertIn("+sbwt none", joined)

    def test_broker_image_is_slim(self):
        self.assertEqual(suite.BROKER_IMAGE, "debian:bookworm-slim")


class TestStepGuards(unittest.TestCase):
    """Check step timeout, schedulers and overload guards."""

    def test_timeout_is_step_plus_120(self):
        self.assertEqual(suite.step_timeout_secs(40), 160)

    def test_scheduler_bounds(self):
        self.assertEqual(suite.scheduler_count("0-9", 1), 10)
        self.assertEqual(suite.scheduler_count("0-9", 20), 4)
        flags = suite.gen_runtime_flags("0-9", 20)
        self.assertIn("+S 4", flags)
        self.assertIn("+sbwt none", flags)

    def test_overload_threshold(self):
        self.assertTrue(suite.is_overloaded(73.0, 24))
        self.assertFalse(suite.is_overloaded(72.0, 24))
        self.assertFalse(suite.is_overloaded(None, 24))

    def test_read_load1(self):
        with mock.patch.object(suite, "read_file", return_value="73.5 12.0 5.0 1/2 3"):
            self.assertEqual(suite.read_load1(), 73.5)

    def test_sampler_flags_after_30s(self):
        sam = suite.Sampler(None, {}, every=5.0, permitted_cores=10)
        for _ in range(6):
            sam.note_load(40.0)
        self.assertTrue(sam.overloaded)
        sam.note_load(1.0)
        sam2 = suite.Sampler(None, {}, every=5.0, permitted_cores=10)
        sam2.note_load(40.0)
        self.assertFalse(sam2.overloaded)

    def test_sleep_guarded_timeout_and_overload(self):
        with mock.patch.object(suite.time, "sleep", return_value=None):
            with mock.patch.object(suite.time, "monotonic", return_value=1000.0):
                self.assertEqual(suite.sleep_guarded(5, 900.0, None), "step timeout")
            sam = suite.Sampler(None, {}, every=5.0, permitted_cores=10)
            sam.overloaded = True
            with mock.patch.object(suite.time, "monotonic", side_effect=[0.0, 0.0, 0.0, 0.0]):
                self.assertEqual(suite.sleep_guarded(5, 9000.0, sam), "host overload")

    def test_abort_row_carries_error(self):
        probe = mock.Mock()
        sc = suite.SCENARIOS["s1-qos1"]
        step = {"label": "r1", "payload": 128, "offered": 100}
        args = mock.Mock(step_secs=40, warm=10)
        broker = mock.Mock()
        broker.name = "bs-broker"
        with mock.patch.object(suite, "shq", return_value=FakeCompleted()):
            row = suite.abort_step(args, sc, broker, step, ["bs-sub"], "bs-pub", None, probe,
                                   "step timeout")
        self.assertEqual(row["error"], "step timeout")
        self.assertEqual(row["verdict"], "fail")
        probe.kill.assert_called_once()

    def test_fanin_counts_and_topics(self):
        sc = suite.FanIn()
        subs = sc.sub_commands("/bin/gen")
        pubs = sc.pub_commands({"interval_ms": 50, "payload": 128}, "/bin/gen")
        self.assertEqual(len(subs), 20)
        self.assertEqual(len(pubs), 20)
        for k in range(1, 21):
            self.assertIn("-c", subs[k - 1].split())
            self.assertIn("$share/g%d/fanin/%d/+" % (k, k), subs[k - 1])
            self.assertIn("fanin/%d/" % k, pubs[k - 1])
        self.assertEqual(sc.groups * sc.subs_per_group, 100)
        self.assertEqual(sc.groups * sc.pubs_per_group, 2000)

    def test_subscribers_start_in_sequence(self):
        sc = suite.FanIn()
        broker = mock.Mock()
        args = mock.Mock(bench_bin="/bin/gen", bench_image="img:1")
        order = []
        with mock.patch.object(suite, "start_generator",
                               side_effect=lambda n, c, cmds, img: order.append(n) or "cid-" + n):
            with mock.patch.object(suite.time, "sleep", return_value=None):
                with mock.patch.object(suite, "wait_for_subscriptions", return_value=True) as wf:
                    with mock.patch.object(suite, "sh", return_value="cid-bs-sub-1"):
                        name, cid, names = suite.start_subscribers(args, sc, broker)
        self.assertEqual(len(order), 20)
        self.assertEqual(order[0], "bs-sub-1")
        self.assertEqual(name, "bs-sub-1")
        wf.assert_called_once()
        self.assertEqual(wf.call_args[0][1], 100)

    def test_subscribers_stop_without_connect(self):
        sc = suite.FanIn()
        broker = mock.Mock()
        args = mock.Mock(bench_bin="/bin/gen", bench_image="img:1")
        with mock.patch.object(suite, "start_generator", return_value="cid"):
            with mock.patch.object(suite.time, "sleep", return_value=None):
                with mock.patch.object(suite, "wait_for_subscriptions", return_value=False):
                    name, cid, names = suite.start_subscribers(args, sc, broker)
        self.assertIsNone(name)
        self.assertIsNone(cid)
        self.assertEqual(len(names), 20)


class TestCleanup(unittest.TestCase):
    """Check container cleanup and process group."""

    def test_list_uses_label_filter(self):
        seen = {}

        def fake_run(*args, **kwargs):
            flat = args[0] if args and isinstance(args[0], list) else list(args)
            seen["args"] = flat
            return FakeCompleted(0, "a\nb\n", "")

        with mock.patch.object(suite.subprocess, "run", side_effect=fake_run):
            out = suite.list_temp_containers()
        self.assertEqual(out, ["a", "b"])
        self.assertIn("label=purpose=benchmark-temp", " ".join(seen["args"]))

    def test_cleanup_removes_label_containers(self):
        calls = []

        def fake_shq(*args):
            calls.append(list(args))
            return FakeCompleted()

        with mock.patch.object(suite, "list_temp_containers", return_value=["a"]):
            with mock.patch.object(suite, "shq", side_effect=fake_shq):
                suite.cleanup_temp_containers()
        flat = " ".join(" ".join(c) for c in calls)
        self.assertIn("rm", flat)
        self.assertIn("bs-pub", flat)

    def test_kill_tracked_probes(self):
        suite._TRACKED_PROBES.clear()
        probe = mock.Mock()
        suite.track_probe(probe)
        suite.kill_tracked_probes()
        probe.kill.assert_called_once()
        self.assertEqual(suite._TRACKED_PROBES, [])

    def test_process_group(self):
        with mock.patch.object(os, "setpgrp", return_value=None) as sp:
            suite.ensure_process_group()
        sp.assert_called_once()

    def test_probe_starts_new_session(self):
        seen = {}

        def fake_popen(cmd, **kwargs):
            seen.update(kwargs)
            return mock.Mock()

        args = mock.Mock(no_probe=False, probe_pairs=20, probe_rate=50)
        with mock.patch.object(suite.subprocess, "Popen", side_effect=fake_popen):
            suite.run_probe(args, 40, 10, 1, "r1")
        self.assertTrue(seen.get("start_new_session"))

    def test_install_handlers(self):
        with mock.patch("atexit.register") as reg:
            with mock.patch.object(signal, "signal") as sig:
                suite.install_cleanup_handlers()
        reg.assert_called_once()
        nums = [c[0][0] for c in sig.call_args_list]
        self.assertIn(signal.SIGTERM, nums)


class TestScriptsAndLoadgen(unittest.TestCase):
    """Check scripts and generator settings."""

    def test_require_loadgen_names_both(self):
        with self.assertRaises(SystemExit) as cm:
            suite.require_loadgen(None, None)
        text = str(cm.exception)
        self.assertIn("LOADGEN_IMAGE", text)
        self.assertIn("LOADGEN_BIN", text)

    def test_require_loadgen_ok(self):
        self.assertEqual(suite.require_loadgen("i", "b"), ("i", "b"))

    def _read(self, name):
        with open(os.path.join(HERE, name)) as f:
            return f.read()

    def test_run_all_new_call(self):
        text = self._read("run_all.sh")
        self.assertIn("<source-dir>", text)
        self.assertIn("<commit-label>", text)
        self.assertIn("<outdir>", text)
        self.assertNotIn("git checkout", text)
        self.assertIn("LOADGEN_IMAGE", text)
        self.assertIn("LOADGEN_BIN", text)
        self.assertIn("build-probe.sh", text)
        self.assertIn("latency-probe", text)

    def test_build_broker_pinned(self):
        text = self._read("build-broker.sh")
        self.assertIn("rust:1.96-bookworm", text)
        self.assertIn("indra-cargo-registry", text)
        self.assertNotIn("git checkout", text)
        self.assertIn("40", text)
        self.assertIn("cpuset", text.lower())
        self.assertIn("target", text)

    def test_build_probe_no_fixed_cpuset(self):
        text = self._read("build-probe.sh")
        self.assertNotIn("12-17", text)
        self.assertIn("rust:1.96-bookworm", text)


if __name__ == "__main__":
    unittest.main()
