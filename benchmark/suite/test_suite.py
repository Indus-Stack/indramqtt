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

    def test_broker_image_is_bench_base(self):
        self.assertEqual(suite.BROKER_IMAGE, "indra-bench-base")
        self.assertEqual(suite.BROKER_IMAGE_BASE, "debian:bookworm-slim")


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
                               side_effect=lambda n, c, cmds, img, mem=None: order.append(n) or "cid-" + n):
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


class TestMemoryLimits(unittest.TestCase):
    """Check host limit, broker share and generator budget."""

    def test_reads_cgroup_limit(self):
        with mock.patch.object(suite, "read_file", return_value="12884901888\n"):
            self.assertEqual(suite.read_host_memory_limit_bytes(), 12884901888)

    def test_max_falls_back_to_memtotal(self):
        def fake(path):
            if path == suite.HOST_MEMORY_MAX_PATH:
                return "max\n"
            return "MemTotal:       25165824 kB\n"
        with mock.patch.object(suite, "read_file", side_effect=fake):
            self.assertEqual(suite.read_host_memory_limit_bytes(), 25165824 * 1024)

    def test_broker_share(self):
        gib = 1024 ** 3
        self.assertEqual(suite.broker_memory_limit_bytes(12 * gib), 6 * gib)
        self.assertEqual(suite.broker_memory_limit_bytes(24 * gib), 8 * gib)

    def test_format_docker_mem(self):
        self.assertEqual(suite.format_docker_mem(6 * 1024 ** 3), "6144m")

    def test_generator_budget_fits(self):
        gib = 1024 ** 3
        host = 12 * gib
        broker = suite.broker_memory_limit_bytes(host)
        per = suite.generator_memory_limit_bytes(host, broker, 21)
        self.assertGreater(per, 0)
        total = broker + 21 * per + suite.PROBE_RESERVE_BYTES
        self.assertLessEqual(total, int(host * 0.85))

    def test_broker_start_sets_limits(self):
        seen = {}

        def fake_docker(*args):
            seen["args"] = list(args)
            return "ok"

        with mock.patch.object(suite, "read_file", return_value="12884901888\n"):
            broker = suite.Broker("/bin/x", "/tmp", 28083, False, 0)

            def fake_sh(*args):
                if "{{.State.Pid}}" in " ".join(args):
                    return "1234"
                return "cid123"

            with mock.patch.object(suite, "docker_run", side_effect=fake_docker):
                with mock.patch.object(suite, "sh", side_effect=fake_sh):
                    with mock.patch.object(suite.time, "sleep", return_value=None):
                        with mock.patch.object(suite.Broker, "api", return_value={}):
                            with mock.patch.object(suite.urllib.request, "urlopen") as url:
                                url.return_value.__enter__.return_value = mock.Mock()
                                broker.start()
        flat = " ".join(seen["args"])
        self.assertIn("--memory=6144m", flat)
        self.assertIn("--memory-swap=6144m", flat)
        self.assertEqual(broker.memory_limit_str, "6144m")

    def test_generator_flags(self):
        seen = {}

        def fake_docker(*args):
            seen["args"] = list(args)
            return "ok"

        with mock.patch.object(suite, "docker_run", side_effect=fake_docker):
            with mock.patch.object(suite, "sh", return_value="cid"):
                suite.start_generator("bs-t", "0-3", ["loadgen sub"], "img:1",
                                      512 * suite.MIB)
        flat = " ".join(seen["args"])
        self.assertIn("--memory=512m", flat)
        self.assertIn("--memory-swap=512m", flat)


class TestHostMemoryGuard(unittest.TestCase):
    """Check sampler guard, error row and peaks."""

    def test_hit_above_90pct(self):
        sam = suite.Sampler(None, {}, host_limit=1000)
        sam.check_host_memory({"host_mem": 950, "mem": 100, "gen_mem": {"pub": 50}})
        self.assertTrue(sam.host_hit)
        self.assertEqual(sam.container_mem["pub"], 50)

    def test_no_hit_below(self):
        sam = suite.Sampler(None, {}, host_limit=1000)
        sam.check_host_memory({"host_mem": 800, "mem": 100, "gen_mem": {}})
        self.assertFalse(sam.host_hit)

    def test_no_hit_unknown(self):
        sam = suite.Sampler(None, {}, host_limit=1000)
        sam.check_host_memory({"host_mem": None, "mem": 0, "gen_mem": {}})
        self.assertFalse(sam.host_hit)

    def test_sleep_guarded_host_memory(self):
        sam = suite.Sampler(None, {}, host_limit=1000)
        sam.host_hit = True
        with mock.patch.object(suite.time, "monotonic", side_effect=[0.0, 0.0, 0.0, 0.0]):
            self.assertEqual(suite.sleep_guarded(5, 9000.0, sam), "host memory")

    def test_error_row_carries_mem(self):
        sam = mock.Mock(broker_peak=100 * 1048576, host_peak=900,
                        gen_peaks={"pub": 50 * 1048576},
                        container_mem={"broker": 100 * 1048576})
        broker = mock.Mock(memory_limit_str="6144m")
        sc = suite.SCENARIOS["s1-qos1"]
        step = {"label": "r1", "payload": 128, "offered": 100}
        row = suite.make_error_row(sc, step, 40, 10, "host memory",
                                   broker=broker, sampler=sam, oom=["bs-pub"])
        self.assertEqual(row["error"], "host memory")
        self.assertEqual(row["oom"], ["bs-pub"])
        self.assertEqual(row["broker_memory"], "6144m")
        self.assertEqual(row["broker_peak_mib"], 100.0)
        self.assertIn("pub", row["gen_peaks_mib"])
        self.assertIn("broker", row["container_mem_mib"])

    def test_gen_peak_mib(self):
        rows = [{"gen_mem": {"pub": 100 * 1048576}},
                {"gen_mem": {"pub": 150 * 1048576, "sub": 10 * 1048576}}]
        peaks = suite.gen_peak_mib(rows)
        self.assertEqual(peaks["pub"], 150.0)
        self.assertEqual(peaks["sub"], 10.0)


class TestStepAlarm(unittest.TestCase):
    """Check alarm arm, handler and one-call kill."""

    def tearDown(self):
        suite._ALARM_FIRED = False
        try:
            suite.disarm_step_alarm()
        except Exception:
            pass

    def test_kill_once(self):
        calls = []

        def fake_shq(*args):
            calls.append(list(args))
            return FakeCompleted()

        with mock.patch.object(suite, "list_temp_containers", return_value=["a", "b"]):
            with mock.patch.object(suite, "shq", side_effect=fake_shq):
                ids = suite.kill_label_containers_once()
        self.assertEqual(ids, ["a", "b"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][:2], ["docker", "kill"])
        self.assertIn("a", calls[0])
        self.assertIn("b", calls[0])

    def test_kill_empty_skips(self):
        with mock.patch.object(suite, "list_temp_containers", return_value=[]):
            with mock.patch.object(suite, "shq") as shq:
                suite.kill_label_containers_once()
        shq.assert_not_called()

    def test_arm_sets_alarm(self):
        with mock.patch.object(signal, "signal") as sig:
            with mock.patch.object(signal, "alarm") as alarm:
                suite.arm_step_alarm(160)
        alarm.assert_called_once_with(160)
        nums = [c[0][0] for c in sig.call_args_list]
        self.assertIn(signal.SIGALRM, nums)

    def test_handler_kills_once_and_flags(self):
        with mock.patch.object(suite, "kill_label_containers_once") as kill:
            suite._step_alarm_handler(signal.SIGALRM, None)
        kill.assert_called_once()
        self.assertTrue(suite._ALARM_FIRED)


class TestOomReport(unittest.TestCase):
    """Check OOM flags and verdict mapping."""

    def test_container_oom_true(self):
        with mock.patch.object(suite, "sh", return_value="true\n"):
            self.assertTrue(suite.container_oom_killed("bs-pub"))
        with mock.patch.object(suite, "sh", return_value="false\n"):
            self.assertFalse(suite.container_oom_killed("bs-pub"))

    def test_collect_oom(self):
        with mock.patch.object(suite, "container_oom_killed",
                               side_effect=lambda n: n == "bs-pub"):
            self.assertEqual(suite.collect_oom(["bs-broker", "bs-pub"]), ["bs-pub"])

    def test_broker_oom_fails(self):
        row = {"delivery_ratio": 1.0, "broker_alive": True, "broker_oom": True,
               "generator_limited": False, "mem_first_mib": 100.0,
               "mem_mib": 102.0, "drain_secs": 2.0}
        self.assertTrue(suite.is_failing(row))
        _, failed = suite.evaluate_step(row)
        self.assertIn("broker-oom", failed)

    def test_generator_flag_fails(self):
        row = {"delivery_ratio": 1.0, "broker_alive": True,
               "generator_limited": True, "mem_first_mib": 100.0,
               "mem_mib": 102.0, "drain_secs": 2.0}
        _, failed = suite.evaluate_step(row)
        self.assertIn("generator-limited", failed)


class TestBenchBaseImage(unittest.TestCase):
    """Check broker base image and its build."""

    def test_dockerfile(self):
        with open(os.path.join(HERE, "Dockerfile")) as f:
            text = f.read()
        self.assertIn("debian:bookworm-slim", text)
        for pkg in ("libssl3", "zlib1g", "libzstd1", "ca-certificates"):
            self.assertIn(pkg, text)

    def test_run_all_builds_base(self):
        with open(os.path.join(HERE, "run_all.sh")) as f:
            text = f.read()
        self.assertIn("indra-bench-base", text)
        self.assertIn("Dockerfile", text)
        self.assertIn("docker build", text)


class TestFanRatesAndStop(unittest.TestCase):
    """Check low start rates and stop on error."""

    def test_fanin_starts_low(self):
        rates = suite.FanIn().default_rates
        self.assertEqual(rates[0], 2000)
        self.assertEqual(sorted(rates), rates)
        self.assertGreater(rates[-1], rates[0])

    def test_fanout_starts_low(self):
        rates = suite.FanOut().default_rates
        self.assertEqual(rates[0], 2000)
        self.assertEqual(sorted(rates), rates)
        self.assertGreater(rates[-1], rates[0])

    def test_error_stops_scenario(self):
        import tempfile as _tf
        with _tf.TemporaryDirectory() as out:
            args = mock.Mock(scenario="s3-fanin", rates="2000,4000", out=out,
                             binary="/bin/x", step_secs=40, warm=10, sub_wait=0,
                             api_port=28083, syscount=False, profile=0,
                             bench_image="img", bench_bin="/bin/gen")
            broker = mock.Mock(name="broker")
            broker.name = "bs-broker"
            broker.start.return_value = None
            broker.alive.return_value = True
            row = {"error": "host memory", "verdict": "fail"}
            with mock.patch.object(suite, "refresh_cpus", return_value={}):
                with mock.patch.object(suite, "Broker", return_value=broker):
                    with mock.patch.object(suite, "sh", return_value=""):
                        with mock.patch.object(suite, "collect_env", return_value={}):
                            with mock.patch.object(suite, "start_subscribers",
                                                   return_value=("bs-sub", "cid", ["bs-sub"])):
                                with mock.patch.object(suite.time, "sleep", return_value=None):
                                    with mock.patch.object(suite, "shq",
                                                           return_value=FakeCompleted()):
                                        with mock.patch.object(suite, "run_step",
                                                               return_value=row) as rs:
                                            with mock.patch.object(suite, "write_outputs"):
                                                rows = suite.run_scenario(args)
            self.assertEqual(rs.call_count, 1)
            self.assertEqual(len(rows), 1)


if __name__ == "__main__":
    unittest.main()
