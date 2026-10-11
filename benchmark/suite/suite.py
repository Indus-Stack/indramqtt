#!/usr/bin/env python3
"""Benchmark suite driver. It measures one broker on a fixed budget.

It runs one scenario as steps against a fresh broker. The broker runs
in a container with 4 pinned cores and 8 GiB. Load generators run in
other containers on other cores. A latency probe runs on its own core.
See README.md for the method.

Usage:
  suite.py list
  suite.py run <scenario> --binary <broker binary> --out <dir> [options]
  suite.py summarize <outdir>
  suite.py compare <newdir> <olddir> --out <csvfile>

Output per scenario: <out>/<scenario>.jsonl (one JSON line per step),
<out>/<scenario>.csv, <out>/<scenario>.summary.json.
"""
import argparse
import csv
import json
import os
import re
import shlex
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from cpu_layout import format_cpu_list, parse_cpu_list
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
LABEL = "purpose=benchmark-temp"
BROKER_IMAGE = "debian:bookworm-slim"
GEN_RUNTIME_BASE = "+sbwt none +sbwtdcpu none +sbwtdio none"
ADMIN_PW = "Adm1n-test-pass!"
API = "http://127.0.0.1:%d/api/v5"
PAYLOAD_DEFAULT = 128
MIN_PERMITTED_CORES = 10
EFFECTIVE_CPUSET_PATH = "/sys/fs/cgroup/cpuset.cpus.effective"
LOADAVG_PATH = "/proc/loadavg"
BROKER_CORES = 4


def read_file(path):
    try:
        with open(path) as f:
            return f.read()
    except OSError:
        return ""


def read_effective_cpus():
    """Read permitted cores from the cgroup file or affinity."""
    text = read_file(EFFECTIVE_CPUSET_PATH).strip()
    if text:
        try:
            cores = parse_cpu_list(text)
            if cores:
                return cores
        except ValueError:
            pass
    try:
        return sorted(os.sched_getaffinity(0))
    except Exception:
        return []


def build_cpu_layout(permitted):
    """Assign broker, probe, publisher and subscriber cores."""
    cores = sorted(set(permitted))
    if len(cores) < MIN_PERMITTED_CORES:
        raise SystemExit(
            "Need at least %d permitted cores, found %d. "
            "Set CPUs with BENCH_BROKER_CPUS, BENCH_PUB_CPUS, "
            "BENCH_SUB_CPUS and BENCH_PROBE_CPUS." % (MIN_PERMITTED_CORES, len(cores)))
    broker = cores[:4]
    probe = cores[4:5]
    rest = cores[5:]
    half = len(rest) // 2
    pub = rest[:half]
    sub = rest[half:]
    return {
        "broker": format_cpu_list(broker),
        "probe": format_cpu_list(probe),
        "pub": format_cpu_list(pub),
        "sub": format_cpu_list(sub),
    }


def resolve_cpus():
    """Resolve CPU specs from the host or the override variables."""
    permitted = read_effective_cpus()
    if len(permitted) < MIN_PERMITTED_CORES:
        raise SystemExit(
            "Need at least %d permitted cores, found %d. "
            "Set CPUs with BENCH_BROKER_CPUS, BENCH_PUB_CPUS, "
            "BENCH_SUB_CPUS and BENCH_PROBE_CPUS." % (MIN_PERMITTED_CORES, len(permitted)))
    defaults = build_cpu_layout(permitted)
    return {
        "broker": os.environ.get("BENCH_BROKER_CPUS") or defaults["broker"],
        "pub": os.environ.get("BENCH_PUB_CPUS") or defaults["pub"],
        "sub": os.environ.get("BENCH_SUB_CPUS") or defaults["sub"],
        "probe": os.environ.get("BENCH_PROBE_CPUS") or defaults["probe"],
    }


CPUS = resolve_cpus()


def refresh_cpus():
    """Refresh global CPU specs from the host. Stop when too few."""
    global CPUS
    CPUS = resolve_cpus()
    return CPUS


def require_loadgen(image, binary):
    """Check generator settings. Stop when a variable is missing."""
    if image and binary:
        return image, binary
    raise SystemExit(
        "Set LOADGEN_IMAGE and LOADGEN_BIN. "
        "Missing image or binary.")


def step_timeout_secs(step_secs):
    """Return the hard time limit of one step."""
    return int(step_secs) + 120


def scheduler_count(cpus_spec, num_vms):
    """Return the scheduler count for one generator VM."""
    total = ncpus(cpus_spec) if cpus_spec else 0
    if total <= 0:
        return 1
    if num_vms is not None and num_vms > 4:
        return max(1, min(total, 4))
    return max(1, total)


def gen_runtime_flags(cpus_spec, num_vms):
    """Return ERL_AFLAGS with a bounded scheduler count."""
    return "%s +S %d" % (GEN_RUNTIME_BASE, scheduler_count(cpus_spec, num_vms))


def read_load1():
    """Read the 1-minute load average. Return None when unknown."""
    try:
        text = read_file(LOADAVG_PATH).strip()
        return float(text.split()[0]) if text else None
    except (OSError, ValueError, IndexError):
        return None


def is_overloaded(load1, num_cores):
    """Check if the load exceeds three times the core count."""
    if load1 is None or not num_cores:
        return False
    return load1 > 3.0 * num_cores


def ncpus(spec):
    """Count cores in a spec like "4-9,21-24"."""
    if not spec:
        return 0
    n = 0
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            n += int(b) - int(a) + 1
        else:
            n += 1
    return n


def sh(*args, input=None, timeout=None):
    """Run a command. Return stdout only."""
    r = subprocess.run(list(args), capture_output=True, text=True, input=input, timeout=timeout)
    return r.stdout


def shq(*args):
    return subprocess.run(list(args), capture_output=True, text=True)


def docker_run(*args):
    """Run a docker run command. Raise with stderr when it fails."""
    r = subprocess.run(list(args), capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError("docker run failed: %s" % (r.stderr.strip() or r.stdout.strip()))
    return r.stdout


def list_temp_containers():
    """List benchmark containers. Return ids."""
    r = shq("docker", "ps", "-aq", "--filter", "label=" + LABEL)
    if r.returncode != 0:
        return []
    return [x for x in r.stdout.split() if x]


def cleanup_temp_containers():
    """Remove all benchmark containers."""
    ids = list_temp_containers()
    if ids:
        shq("docker", "rm", "-f", *ids)
    shq("docker", "rm", "-f", "bs-broker", "bs-pub", "bs-sub")
    for i in range(1, 64):
        shq("docker", "rm", "-f", "bs-sub-%d" % i)


_TRACKED_PROBES = []


def track_probe(p):
    """Track a probe process for cleanup."""
    if p is not None:
        _TRACKED_PROBES.append(p)
    return p


def kill_tracked_probes():
    """Kill tracked probe processes."""
    for p in list(_TRACKED_PROBES):
        try:
            p.kill()
        except Exception:
            pass
    _TRACKED_PROBES.clear()


def cleanup_all():
    """Remove containers and kill the probe."""
    try:
        kill_tracked_probes()
    finally:
        cleanup_temp_containers()


def ensure_process_group():
    """Put the suite in its own process group."""
    try:
        os.setpgrp()
    except Exception:
        pass


def install_cleanup_handlers():
    """Remove containers on exit and on termination signals."""
    import atexit
    import signal
    atexit.register(cleanup_all)

    def _on_signal(signum, _frame):
        cleanup_all()
        raise SystemExit("Stopped by signal %d." % signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, _on_signal)
        except Exception:
            pass


def subscription_count(broker):
    """Count subscriptions through the broker API."""
    try:
        r = broker.api("GET", "/subscriptions")
    except Exception:
        return 0
    if isinstance(r, list):
        return len(r)
    if isinstance(r, dict):
        for key in ("data", "subscriptions", "items"):
            v = r.get(key)
            if isinstance(v, list):
                return len(v)
        if "error" in r:
            return 0
    return 0


def wait_for_subscriptions(broker, expected, timeout=60, pause=2.0):
    """Wait until the broker shows the expected subscription count."""
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if subscription_count(broker) >= expected:
            return True
        time.sleep(pause)
    return subscription_count(broker) >= expected


# ------------------------------------------------------------- scenarios

def rate_to_interval(target, clients):
    """Interval in ms for `clients` publishers that gives `target` msg/s."""
    return max(1, round(clients * 1000.0 / target))


class Scenario:
    """A scenario holds subscribers and a list of steps."""

    name = ""
    desc = ""
    qos = 0
    fanout = 1
    default_rates = []
    payload = PAYLOAD_DEFAULT

    def sub_commands(self, bench_bin):
        raise NotImplementedError

    def pub_commands(self, step, bench_bin):
        raise NotImplementedError

    def steps(self, rates):
        return [self.make_step(r) for r in rates]

    def make_step(self, rate):
        raise NotImplementedError


def gen(role, bench_bin, *opts):
    return shlex.join([bench_bin, role] + [str(o) for o in opts])


class Symmetric(Scenario):
    """1000 publishers send to 1000 subscribers on 1000 topics."""

    clients = 1000

    def __init__(self, qos, rates):
        self.qos = qos
        self.name = "s1-qos%d" % qos
        self.desc = "Symmetric: 1000 publishers, 1000 subscribers, 1000 topics, QoS %d, 128 B" % qos
        self.default_rates = rates

    def make_step(self, rate):
        iv = rate_to_interval(rate, self.clients)
        return {"label": "r%d" % rate, "interval_ms": iv, "payload": self.payload,
                "offered": round(self.clients * 1000.0 / iv)}

    def sub_commands(self, bench_bin):
        return [gen("sub", bench_bin, "-h", "127.0.0.1", "-p", 1883, "-V", 4, "-c", self.clients, "-i", 1,
                    "-t", "t/%i", "-q", self.qos, "--payload-hdrs", "ts")]

    def pub_commands(self, step, bench_bin):
        return [gen("pub", bench_bin, "-h", "127.0.0.1", "-p", 1883, "-V", 4, "-c", self.clients, "-i", 1,
                    "-I", step["interval_ms"], "-t", "t/%i", "-q", self.qos, "-s", step["payload"],
                    "--payload-hdrs", "ts", "-w", "true")]


class PayloadSweep(Symmetric):
    """Fixed 20000 msg/s inbound at QoS 1 with four payload sizes."""

    def __init__(self):
        super().__init__(1, [])
        self.name = "s2-payload"
        self.desc = "Payload sweep: 20000 msg/s inbound, QoS 1, 128 B / 1 KiB / 4 KiB / 8 KiB"
        self.default_rates = [128, 1024, 4096, 8192]

    def make_step(self, size):
        return {"label": "p%d" % size, "interval_ms": 50, "payload": int(size), "offered": 20000}


class FanIn(Scenario):
    """2000 publishers send. 100 subscribers in 20 shared groups read."""

    groups = 20
    pubs_per_group = 100
    subs_per_group = 5
    qos = 1

    def __init__(self):
        self.name = "s3-fanin"
        self.desc = ("Fan-in: 2000 publishers, 100 subscribers in 20 shared groups of 5, "
                     "each group reads 100 publishers, QoS 1, 128 B")
        self.default_rates = [10000, 20000, 30000, 40000, 60000, 80000]

    def make_step(self, rate):
        total_pubs = self.groups * self.pubs_per_group
        iv = rate_to_interval(rate, total_pubs)
        return {"label": "r%d" % rate, "interval_ms": iv, "payload": self.payload,
                "offered": round(total_pubs * 1000.0 / iv)}

    def sub_commands(self, bench_bin):
        cmds = []
        for k in range(1, self.groups + 1):
            cmds.append(gen("sub", bench_bin, "-h", "127.0.0.1", "-p", 1883, "-V", 4, "-c", self.subs_per_group,
                            "-i", 1, "-t", "$share/g%d/fanin/%d/+" % (k, k), "-q", self.qos,
                            "--prefix", "fis%d" % k, "--payload-hdrs", "ts"))
        return cmds

    def pub_commands(self, step, bench_bin):
        cmds = []
        for k in range(1, self.groups + 1):
            cmds.append(gen("pub", bench_bin, "-h", "127.0.0.1", "-p", 1883, "-V", 4, "-c", self.pubs_per_group,
                            "-i", 1, "-I", step["interval_ms"], "-t", "fanin/%d/%%i" % k,
                            "-q", self.qos, "-s", step["payload"], "--prefix", "fip%d" % k,
                            "--payload-hdrs", "ts", "-w", "true"))
        return cmds


class FanOut(Scenario):
    """10 publishers send. 2000 subscribers receive. 200 share a topic."""

    topics = 10
    subs_per_topic = 200
    qos = 1
    fanout = 200

    def __init__(self):
        self.name = "s3-fanout"
        self.desc = "Fan-out: 10 publishers, 2000 subscribers, 200 per topic, QoS 1, 128 B"
        self.default_rates = [250, 500, 750, 1000, 1500, 2000]

    def make_step(self, rate):
        iv = rate_to_interval(rate, self.topics)
        return {"label": "r%d" % rate, "interval_ms": iv, "payload": self.payload,
                "offered": round(self.topics * 1000.0 / iv)}

    def sub_commands(self, bench_bin):
        cmds = []
        for k in range(1, self.topics + 1):
            cmds.append(gen("sub", bench_bin, "-h", "127.0.0.1", "-p", 1883, "-V", 4, "-c", self.subs_per_topic,
                            "-i", 1, "-t", "fanout/%d" % k, "-q", self.qos,
                            "--prefix", "fos%d" % k, "--payload-hdrs", "ts"))
        return cmds

    def pub_commands(self, step, bench_bin):
        cmds = []
        for k in range(1, self.topics + 1):
            cmds.append(gen("pub", bench_bin, "-h", "127.0.0.1", "-p", 1883, "-V", 4, "-c", 1, "-i", 1,
                            "-I", step["interval_ms"], "-t", "fanout/%d" % k, "-q", self.qos,
                            "-s", step["payload"], "--prefix", "fop%d" % k,
                            "--payload-hdrs", "ts", "-w", "true"))
        return cmds


SCENARIOS = {s.name: s for s in [
    Symmetric(0, [20000, 50000, 100000, 125000, 166666, 200000, 250000]),
    Symmetric(1, [10000, 20000, 30000, 40000, 60000, 80000, 100000]),
    Symmetric(2, [5000, 10000, 20000, 30000, 40000, 50000]),
    PayloadSweep(),
    FanIn(),
    FanOut(),
]}
SCENARIOS["s1-qos0"].name = "s1-qos0"


# -------------------------------------------------------- step verdicts

# A step fails when delivery drops or the broker stops. The driver
# stops a scenario after two failing steps in a row.
DELIVERY_FAIL_BELOW = 0.9
# Memory is flat when growth stays below 15 MiB or 4 percent.
MEM_FLAT_MIB = 15.0
MEM_FLAT_RATIO = 0.04
# Latency is flat when the median of the last third stays below two
# times the median of the first third plus 50 ms.
LAT_FLAT_FACTOR = 2.0
LAT_FLAT_ADD_MS = 50.0


def memory_flat(mem_first_mib, mem_last_mib):
    """Check that memory does not grow during a step."""
    if mem_first_mib is None or mem_last_mib is None:
        return None
    growth = mem_last_mib - mem_first_mib
    return growth < max(MEM_FLAT_MIB, MEM_FLAT_RATIO * mem_first_mib)


def latency_flat_from_series(p99_ms):
    """Check that p99 latency does not rise during a step."""
    if not p99_ms or len(p99_ms) < 3:
        return None
    third = max(1, len(p99_ms) // 3)
    first = sorted(p99_ms[:third])[len(p99_ms[:third]) // 2]
    last = sorted(p99_ms[-third:])[len(p99_ms[-third:]) // 2]
    return last <= LAT_FLAT_FACTOR * first + LAT_FLAT_ADD_MS


def is_failing(row):
    """A step fails on low delivery or a stopped broker."""
    if not row.get("broker_alive", True):
        return True
    return (row.get("delivery_ratio") or 0.0) < DELIVERY_FAIL_BELOW


def evaluate_step(row):
    """Rate one step. Return (verdict, failed list)."""
    failed = []
    if (row.get("delivery_ratio") or 0.0) < DELIVERY_FAIL_BELOW:
        failed.append("delivery-below-90pct")
    if not row.get("broker_alive", True):
        failed.append("broker-not-alive")
    if row.get("generator_limited"):
        failed.append("generator-limited")
    mem = memory_flat(row.get("mem_first_mib"), row.get("mem_mib"))
    if mem is False:
        failed.append("memory-rising")
    lat = latency_flat_from_series(row.get("probe_p99_ms_series") or [])
    if lat is False:
        failed.append("latency-rising")
    if row.get("drain_secs") is not None and row["drain_secs"] > 30.0:
        failed.append("drain-too-long")
    verdict = "pass" if not failed else "fail"
    return verdict, failed


def pick_recommended(rows):
    """Pick the step with CPU nearest 75 percent. Skip bad steps."""
    if not rows:
        return None
    ok = [r for r in rows if not r.get("generator_limited")
          and (r.get("delivery_ratio") or 0.0) >= DELIVERY_FAIL_BELOW]
    pool = ok or rows
    return min(pool, key=lambda r: abs((r.get("cpu_pct_of_4") or 0.0) - 75.0))


# ---------------------------------------------------------------- system

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def collect_env(binary):
    """Record CPU, memory, kernel and build identity. Show gaps plainly."""
    info = {"recorded_at": now_iso()}
    try:
        cpu = read_file("/proc/cpuinfo")
        m = re.search(r"model name\s*:\s*(.+)", cpu)
        info["cpu_model"] = m.group(1).strip() if m else "unknown"
        info["cpu_cores"] = len(re.findall(r"^processor\s*:", cpu, re.M))
    except Exception:
        info["cpu_model"] = "unknown"
        info["cpu_cores"] = 0
    mem = read_file("/proc/meminfo")
    m = re.search(r"MemTotal:\s*(\d+)\s*kB", mem)
    info["memory_mib"] = round(int(m.group(1)) / 1024, 1) if m else 0.0
    try:
        info["kernel"] = os.uname().release
    except Exception:
        info["kernel"] = "unknown"
    info["broker_cores"] = BROKER_CORES
    info["broker_memory"] = "8g"
    try:
        h = subprocess.run(["sha256sum", binary], capture_output=True, text=True, timeout=30)
        info["build_sha256"] = h.stdout.split()[0] if h.stdout else "unknown"
    except Exception:
        info["build_sha256"] = "unknown"
    info["cpu_layout"] = dict(CPUS)
    return info


def cgroup_dir_of_container(cid):
    return "/sys/fs/cgroup/system.slice/docker-%s.scope" % cid.strip()


def cpu_stat(cgdir):
    d = {}
    for line in read_file(cgdir + "/cpu.stat").splitlines():
        p = line.split()
        if len(p) == 2:
            try:
                d[p[0]] = int(p[1])
            except ValueError:
                pass
    return d


def proc_io(pid):
    d = {}
    for line in read_file("/proc/%d/io" % pid).splitlines():
        k, _, v = line.partition(":")
        try:
            d[k.strip()] = int(v)
        except ValueError:
            pass
    return d


def proc_ctxt(pid):
    vol = invol = 0
    try:
        for tid in os.listdir("/proc/%d/task" % pid):
            for line in read_file("/proc/%d/task/%s/status" % (pid, tid)).splitlines():
                if line.startswith("voluntary_ctxt_switches"):
                    vol += int(line.split()[1])
                elif line.startswith("nonvoluntary_ctxt_switches"):
                    invol += int(line.split()[1])
    except OSError:
        pass
    return vol, invol


SYSCOUNT_NAMES = ["send", "send_bytes", "recv", "recv_bytes", "read", "read_bytes", "write",
                  "write_bytes", "epoll_wait", "epoll_events", "epoll_ctl", "futex_wait",
                  "futex_wake", "vec_out", "vec_in", "spare"]


def syscount_read(path):
    """Sum the stripes of the shared counter file of the call library."""
    try:
        with open(path, "rb") as f:
            raw = f.read()
    except OSError:
        return None
    n = len(raw) // 8
    if n == 0:
        return None
    vals = struct.unpack("<%dQ" % n, raw[: n * 8])
    out = [0] * 16
    for i, v in enumerate(vals):
        out[i % 16] += v
    return dict(zip(SYSCOUNT_NAMES, out))


class Broker:
    def __init__(self, binary, run_dir, api_port, use_syscount, prof, name="bs-broker"):
        self.binary, self.run_dir, self.api_port = binary, run_dir, api_port
        self.use_syscount, self.prof, self.name = use_syscount, prof, name
        self.token = None
        self.cid = None
        self.pid = None
        self.cg = None

    def api(self, method, path, body=None, retry_login=True):
        req = urllib.request.Request((API % self.api_port) + path, method=method,
                                     data=json.dumps(body).encode() if body is not None else None)
        req.add_header("content-type", "application/json")
        if self.token:
            req.add_header("authorization", "Bearer " + self.token)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as e:
            if e.code == 401 and retry_login and path != "/login":
                self.login()
                return self.api(method, path, body, False)
            return {"error": "http %d" % e.code}
        except Exception as e:
            return {"error": str(e)}

    def login(self):
        r = self.api("POST", "/login", {"username": "admin", "password": ADMIN_PW}, retry_login=False)
        self.token = r.get("token")

    def start(self):
        sh("docker", "rm", "-f", self.name)
        self.syscount_file = os.path.join(self.run_dir, "syscount.bin")
        self.prof_prefix = os.path.join(self.run_dir, "prof")
        env = ["-e", "INDRA_LISTENERS__TCP__NATIVE=true", "-e", "INDRA_LISTENERS__TCP__BIND=0.0.0.0:1883",
               "-e", "INDRA_AUTH__ALLOW_ANONYMOUS=true"]
        mounts = ["-v", "%s:/usr/local/bin/indramqtt:ro" % self.binary, "-v", "%s:/run/bs" % self.run_dir]
        if self.use_syscount:
            lib = os.path.join(HERE, "bin", "libsyscount.so")
            mounts += ["-v", "%s:/usr/local/lib/libsyscount.so:ro" % lib]
            env += ["-e", "LD_PRELOAD=/usr/local/lib/libsyscount.so", "-e", "SYSCOUNT_FILE=/run/bs/syscount.bin"]
        if self.prof:
            env += ["-e", "DIAG_PROF_OUT=/run/bs/prof", "-e", "DIAG_PROF_HZ=%d" % self.prof]
        docker_run("docker", "run", "-d", "--label", LABEL, "--name", self.name, "--network", "host",
                   "--cpus=%d" % BROKER_CORES, "--cpuset-cpus=" + CPUS["broker"], "--memory=8g", "--memory-swap=8g",
                   "--ulimit", "nofile=524288:524288", *env, *mounts, BROKER_IMAGE, "sh", "-c",
                   "mkdir -p /tmp/d && exec /usr/local/bin/indramqtt --api-bind 127.0.0.1:%d --data-dir /tmp/d" % self.api_port)
        ok = False
        for _ in range(80):
            time.sleep(0.5)
            try:
                urllib.request.urlopen("http://127.0.0.1:%d/healthz" % self.api_port, timeout=2)
                ok = True
                break
            except Exception:
                pass
        if not ok:
            logs = sh("docker", "logs", "--tail", "20", self.name)
            ldd = ""
            try:
                lr = subprocess.run(["ldd", self.binary], capture_output=True, text=True, timeout=30)
                ldd = (lr.stdout or "") + (lr.stderr or "")
            except Exception as e:
                ldd = "ldd failed: %s" % e
            raise RuntimeError("broker did not start: %s\nldd %s:\n%s" % (logs, self.binary, ldd))
        self.cid = sh("docker", "inspect", "-f", "{{.Id}}", self.name).strip()
        self.pid = int(sh("docker", "inspect", "-f", "{{.State.Pid}}", self.name).strip())
        self.cg = cgroup_dir_of_container(self.cid)
        r = self.api("POST", "/login", {"username": "admin", "password": "public"}, retry_login=False)
        self.token = r.get("token")
        self.api("POST", "/users/admin/change_pwd", {"old_pwd": "public", "new_pwd": ADMIN_PW})
        self.login()

    def counters(self):
        m = self.api("GET", "/metrics")
        if isinstance(m, list):
            m = m[0] if m else {}
        return {k: v for k, v in m.items() if isinstance(v, (int, float))}

    def alive(self):
        out = sh("docker", "ps", "--format", "{{.Names}}")
        oom = "true" in sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", self.name)
        return self.name in out.split() and not oom

    def stop(self):
        sh("docker", "rm", "-f", self.name)


def snapshot(broker, gens):
    """Take one reading of all counters."""
    s = {"t": time.monotonic(), "wall": time.time()}
    s["api"] = broker.counters()
    s["cpu"] = cpu_stat(broker.cg)
    s["mem"] = int(read_file(broker.cg + "/memory.current").strip() or 0)
    s["io"] = proc_io(broker.pid)
    s["ctxt"] = proc_ctxt(broker.pid)
    s["sc"] = syscount_read(broker.syscount_file) if broker.use_syscount else None
    s["gen"] = {n: cpu_stat(cg).get("usage_usec", 0) for n, cg in gens.items()}
    return s


class Sampler(threading.Thread):
    def __init__(self, broker, gens, every=5.0, permitted_cores=0):
        super().__init__(daemon=True)
        self.broker, self.gens, self.every = broker, gens, every
        self.rows = []
        self.halt = False
        self.permitted_cores = permitted_cores
        self.overload_secs = 0.0
        self.overloaded = False
        self.load1 = None

    def note_load(self, load1):
        """Track overload time. Set the flag after 30 s."""
        self.load1 = load1
        if is_overloaded(load1, self.permitted_cores):
            self.overload_secs += self.every
        else:
            self.overload_secs = 0.0
        if self.overload_secs >= 30.0:
            self.overloaded = True

    def run(self):
        while not self.halt:
            t = time.monotonic()
            try:
                self.rows.append(snapshot(self.broker, self.gens))
            except Exception as e:
                print("sampler:", e, file=sys.stderr)
            try:
                self.note_load(read_load1())
            except Exception as e:
                print("sampler:", e, file=sys.stderr)
            if self.overloaded:
                print("sampler: host overload", file=sys.stderr)
                break
            time.sleep(max(0.0, self.every - (time.monotonic() - t)))


def dropped_total(c):
    return sum(v for k, v in c.items() if "dropped" in k or "shed" in k)


def start_generator(name, cpus, commands, bench_image):
    sh("docker", "rm", "-f", name)
    script = "\n".join(c + " &" for c in commands) + "\nwait\n"
    flags = gen_runtime_flags(cpus, len(commands))
    docker_run("docker", "run", "-d", "--label", LABEL, "--name", name, "--network", "host",
               "--cpuset-cpus=" + cpus, "--ulimit", "nofile=524288:524288",
               "-e", "ERL_AFLAGS=" + flags, "--entrypoint", "sh", bench_image, "-c", script)
    cid = sh("docker", "inspect", "-f", "{{.Id}}", name).strip()
    return cid


def stop_step_containers(broker_name="bs-broker"):
    """Kill generator and broker containers of one step."""
    shq("docker", "rm", "-f", "bs-pub", "bs-sub", broker_name)
    for i in range(1, 64):
        shq("docker", "rm", "-f", "bs-sub-%d" % i)
    kill_tracked_probes()


def gen_latency_from_text(text):
    """Parse mean latency lines of the subscriber log. Values in ms."""
    vals = [int(x) for x in re.findall(r"publish_latency avg=(\d+)ms", text)]
    if not vals:
        return None, None
    return sum(vals) / len(vals), max(vals)


def gen_latency(name, since_iso):
    """Read mean and max of subscriber latency lines since a time."""
    r = shq("docker", "logs", "--since", since_iso, name)
    text = r.stdout + r.stderr
    return gen_latency_from_text(text)


def parse_probe_lines(text):
    """Split probe stdout into interval records and the final record."""
    intervals = []
    final = None
    for line in text.splitlines():
        try:
            j = json.loads(line)
        except ValueError:
            continue
        if j.get("type") == "interval":
            intervals.append(j)
        elif j.get("type") == "final":
            final = j
    return intervals, final


def run_probe(args, step_secs, warm, qos, run_id):
    if args.no_probe:
        return None
    exe = os.path.join(HERE, "bin", "latency-probe")
    cmd = ["taskset", "-c", CPUS["probe"], exe, "--host", "127.0.0.1", "--port", "1883",
           "--pairs", str(args.probe_pairs), "--rate", str(args.probe_rate), "--qos", str(qos),
           "--duration", str(max(10, step_secs - 2)), "--warmup", str(max(0, warm - 1)),
           "--interval", "10", "--run", run_id]
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                         start_new_session=True)
    return track_probe(p)


def probe_result(p):
    if p is None:
        return None, []
    try:
        out, err = p.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        return {"error": "probe timeout"}, []
    intervals, final = parse_probe_lines(out)
    if final is None:
        return {"error": "no final line", "stderr": err[-300:]}, intervals
    return final, intervals


def find_snapshot(rows, t, after=True):
    best = None
    for r in rows:
        if after and r["t"] >= t:
            return r
        if not after and r["t"] <= t:
            best = r
    return best


def delta(a, b, *path):
    def get(s):
        for p in path:
            s = s.get(p, 0) if isinstance(s, dict) else 0
        return s or 0
    return get(b) - get(a)


def permitted_core_count():
    """Count permitted cores for the overload guard."""
    try:
        cores = read_effective_cpus()
        if cores:
            return len(cores)
    except Exception:
        pass
    try:
        return sum(ncpus(CPUS[k]) for k in ("broker", "pub", "sub", "probe") if CPUS.get(k))
    except Exception:
        return 0


def make_error_row(sc, step, secs, warm, message):
    """Make a result row for a stopped step."""
    return {
        "scenario": sc.name, "step": step["label"], "payload": step["payload"],
        "offered": step["offered"], "step_secs": secs, "warm_secs": warm,
        "window_secs": 0.0, "in_rate": 0, "out_rate": 0,
        "delivery_ratio": 0.0, "in_vs_offered": 0.0, "dropped": 0,
        "cpu_pct_of_4": 0.0, "broker_alive": False,
        "generator_limited": False, "drain_secs": 0.0,
        "verdict": "fail", "failed": [message], "error": message,
    }


def sleep_guarded(total, deadline, sampler, poll=1.0):
    """Sleep in chunks. Return "step timeout" or "host overload"."""
    end = time.monotonic() + total
    while time.monotonic() < end:
        if time.monotonic() > deadline:
            return "step timeout"
        if sampler is not None and getattr(sampler, "overloaded", False):
            return "host overload"
        time.sleep(min(poll, max(0.0, end - time.monotonic())))
    if time.monotonic() > deadline:
        return "step timeout"
    if sampler is not None and getattr(sampler, "overloaded", False):
        return "host overload"
    return None


def abort_step(args, sc, broker, step, sub_names, pub_name, sampler, probe, message):
    """Kill step processes and make an error row."""
    try:
        if sampler is not None:
            sampler.halt = True
        if probe is not None:
            try:
                probe.kill()
            except Exception:
                pass
        shq("docker", "rm", "-f", pub_name, *sub_names)
        shq("docker", "rm", "-f", broker.name)
    finally:
        pass
    return make_error_row(sc, step, args.step_secs, args.warm, message)


def fanin_expected_subs(sc):
    """Count expected subscriber connections of a fan-in scenario."""
    try:
        return int(sc.groups) * int(sc.subs_per_group)
    except Exception:
        return 0


def start_subscribers(args, sc, broker):
    """Start subscriber generators. Return names and main cid."""
    cmds = sc.sub_commands(args.bench_bin)
    if isinstance(sc, FanIn):
        names = []
        for k, cmd in enumerate(cmds, 1):
            name = "bs-sub-%d" % k
            start_generator(name, CPUS["sub"], [cmd], args.bench_image)
            names.append(name)
            time.sleep(2.0)
        expected = fanin_expected_subs(sc)
        if expected and not wait_for_subscriptions(broker, expected, timeout=60):
            return None, None, names
        cid = sh("docker", "inspect", "-f", "{{.Id}}", names[0]).strip() if names else ""
        return names[0], cid, names
    name = "bs-sub"
    cid = start_generator(name, CPUS["sub"], cmds, args.bench_image)
    return name, cid, [name]


def run_step(args, sc, broker, step, sub_name, sub_cid, probe_qos, sub_names=None):
    pub_name = "bs-pub"
    probe_id = "%s%d" % (sc.name[:4], int(time.time()) % 100000)
    secs, warm = args.step_secs, args.warm
    subs = list(sub_names) if sub_names else [sub_name]
    gens = {"pub": None, "sub": cgroup_dir_of_container(sub_cid)}
    deadline = time.monotonic() + step_timeout_secs(secs)
    pub_cid = start_generator(pub_name, CPUS["pub"], sc.pub_commands(step, args.bench_bin), args.bench_image)
    gens["pub"] = cgroup_dir_of_container(pub_cid)
    probe = run_probe(args, secs, warm, probe_qos, probe_id)
    sam = Sampler(broker, gens, every=args.sample_every,
                  permitted_cores=permitted_core_count())
    sam.start()
    hit = sleep_guarded(warm, deadline, sam)
    if hit:
        sam.halt = True
        return abort_step(args, sc, broker, step, subs, pub_name, sam, probe, hit)
    w0_wall = now_iso()
    if args.profile and not args.no_prof_window:
        open(broker.prof_prefix + ".start", "w").close()
    s_a = snapshot(broker, gens)
    hit = sleep_guarded(max(1, secs - warm), deadline, sam)
    if hit:
        sam.halt = True
        return abort_step(args, sc, broker, step, subs, pub_name, sam, probe, hit)
    s_b = snapshot(broker, gens)
    if getattr(sam, "overloaded", False):
        sam.halt = True
        return abort_step(args, sc, broker, step, subs, pub_name, sam, probe, "host overload")
    if time.monotonic() > deadline:
        sam.halt = True
        return abort_step(args, sc, broker, step, subs, pub_name, sam, probe, "step timeout")
    if args.profile and not args.no_prof_window:
        open(broker.prof_prefix + ".dump", "w").close()
    shq("docker", "kill", pub_name)
    t_kill = time.monotonic()
    prev = broker.counters().get("messages.delivered", 0)
    for _ in range(30):
        time.sleep(1.0)
        if time.monotonic() > deadline:
            sam.halt = True
            return abort_step(args, sc, broker, step, subs, pub_name, sam, probe, "step timeout")
        if getattr(sam, "overloaded", False):
            sam.halt = True
            return abort_step(args, sc, broker, step, subs, pub_name, sam, probe, "host overload")
        cur = broker.counters().get("messages.delivered", 0)
        if cur == prev:
            break
        prev = cur
    drain = time.monotonic() - t_kill
    c_end = broker.counters()
    sam.halt = True
    time.sleep(0.2)
    pres, intervals = probe_result(probe)
    lat_avg, lat_max = gen_latency(sub_name, w0_wall)
    shq("docker", "rm", "-f", pub_name)

    win = s_b["t"] - s_a["t"]
    pr = args.probe_pairs * args.probe_rate if not args.no_probe else 0
    d_in = delta(s_a, s_b, "api", "messages.received")
    d_out = delta(s_a, s_b, "api", "messages.delivered")
    in_rate = d_in / win - pr
    out_rate = d_out / win - pr
    cpu_user = delta(s_a, s_b, "cpu", "user_usec") / 1e6
    cpu_sys = delta(s_a, s_b, "cpu", "system_usec") / 1e6
    cpu_tot = delta(s_a, s_b, "cpu", "usage_usec") / 1e6
    syscw = delta(s_a, s_b, "io", "syscw")
    syscr = delta(s_a, s_b, "io", "syscr")
    vol = s_b["ctxt"][0] - s_a["ctxt"][0]
    invol = s_b["ctxt"][1] - s_a["ctxt"][1]
    mems = [r["mem"] for r in sam.rows if s_a["t"] <= r["t"] <= s_b["t"]] or [s_b["mem"]]
    out_msgs = max(1.0, d_out)
    in_msgs = max(1.0, d_in)
    pubcpu = delta(s_a, s_b, "gen", "pub") / 1e6 / win
    subcpu = delta(s_a, s_b, "gen", "sub") / 1e6 / win
    pubcap, subcap = ncpus(CPUS["pub"]), ncpus(CPUS["sub"])

    def peak(key, cap):
        rows = [r for r in sam.rows if s_a["t"] <= r["t"] <= s_b["t"]]
        best = 0.0
        for x, y in zip(rows, rows[1:]):
            dt = y["t"] - x["t"]
            if dt > 0:
                best = max(best, (y["gen"].get(key, 0) - x["gen"].get(key, 0)) / 1e6 / dt / cap)
        return best
    gen_pub_peak, gen_sub_peak = peak("pub", pubcap), peak("sub", subcap)
    gen_limited = (pubcpu / pubcap >= 0.90 or subcpu / subcap >= 0.90 or gen_pub_peak >= 0.97
                   or gen_sub_peak >= 0.97)

    row = {
        "scenario": sc.name, "step": step["label"], "payload": step["payload"],
        "offered": step["offered"], "step_secs": secs, "warm_secs": warm, "window_secs": round(win, 1),
        "in_rate": round(in_rate), "out_rate": round(out_rate),
        "delivery_ratio": round(out_rate / max(1.0, in_rate * sc.fanout), 4),
        "in_vs_offered": round(in_rate / step["offered"], 4),
        "dropped": dropped_total(c_end) - dropped_total(s_a["api"]),
        "cpu_pct_of_4": round(cpu_tot / win / BROKER_CORES * 100, 1),
        "cpu_user_pct_of_4": round(cpu_user / win / BROKER_CORES * 100, 1),
        "cpu_sys_pct_of_4": round(cpu_sys / win / BROKER_CORES * 100, 1),
        "cpu_user_share": round(cpu_user / max(1e-9, cpu_user + cpu_sys), 3),
        "mem_mib": round(mems[-1] / 1048576, 1), "mem_peak_mib": round(max(mems) / 1048576, 1),
        "mem_first_mib": round(mems[0] / 1048576, 1),
        "syscw_per_s": round(syscw / win), "syscr_per_s": round(syscr / win),
        "syscw_per_out_msg": round(syscw / out_msgs, 4), "syscr_per_in_msg": round(syscr / in_msgs, 4),
        "ctxt_vol_per_s": round(vol / win), "ctxt_invol_per_s": round(invol / win),
        "ctxt_per_out_msg": round((vol + invol) / out_msgs, 4),
        "lat_gen_avg_ms": None if lat_avg is None else round(lat_avg, 1),
        "lat_gen_max_of_avg_ms": lat_max,
        "gen_pub_cpu_pct_of_cap": round(pubcpu / pubcap * 100, 1),
        "gen_sub_cpu_pct_of_cap": round(subcpu / subcap * 100, 1),
        "gen_pub_peak_pct": round(gen_pub_peak * 100, 1), "gen_sub_peak_pct": round(gen_sub_peak * 100, 1),
        "generator_limited": gen_limited,
        "drain_secs": round(drain, 1), "broker_alive": broker.alive(),
    }
    if pres and "error" not in pres:
        for k in ("count", "lost", "p50_us", "p90_us", "p99_us", "p999_us", "max_us"):
            row["probe_" + k] = pres.get(k)
        series = [ (iv.get("p99_us") or 0) / 1000.0 for iv in intervals if iv.get("p99_us") ]
        if series:
            row["probe_p99_ms_series"] = [round(v, 2) for v in series]
    elif pres:
        row["probe_error"] = pres.get("error")
    if s_a["sc"] and s_b["sc"]:
        d = {k: s_b["sc"][k] - s_a["sc"][k] for k in s_a["sc"]}
        row.update({
            "sc_send_per_out_msg": round(d["send"] / out_msgs, 4),
            "sc_recv_per_in_msg": round(d["recv"] / in_msgs, 4),
            "sc_bytes_per_send": round(d["send_bytes"] / max(1, d["send"]), 1),
            "sc_epoll_wait_per_msg": round(d["epoll_wait"] / (in_msgs + out_msgs), 4),
            "sc_epoll_events_per_wait": round(d["epoll_events"] / max(1, d["epoll_wait"]), 2),
            "sc_futex_wait_per_msg": round(d["futex_wait"] / (in_msgs + out_msgs), 4),
            "sc_futex_wake_per_msg": round(d["futex_wake"] / (in_msgs + out_msgs), 4),
            "sc_send_per_s": round(d["send"] / win), "sc_recv_per_s": round(d["recv"] / win),
        })
    verdict, failed = evaluate_step(row)
    row["verdict"] = verdict
    row["failed"] = failed
    return row


def run_scenario(args):
    refresh_cpus()
    args.bench_image, args.bench_bin = require_loadgen(
        getattr(args, "bench_image", None) or os.environ.get("LOADGEN_IMAGE"),
        getattr(args, "bench_bin", None) or os.environ.get("LOADGEN_BIN"))
    sc = SCENARIOS[args.scenario]
    rates = [float(x) for x in args.rates.split(",")] if args.rates else sc.default_rates
    rates = [int(r) if r == int(r) else r for r in rates]
    os.makedirs(args.out, exist_ok=True)
    run_dir = os.path.abspath(os.path.join(args.out, "run-" + sc.name))
    os.makedirs(run_dir, exist_ok=True)
    binary = os.path.abspath(args.binary)
    broker = Broker(binary, run_dir, args.api_port, args.syscount, args.profile)
    with open(os.path.join(args.out, "env.txt"), "a") as f:
        f.write(sh("sh", "-c", "date -u; uptime; cat /sys/fs/cgroup/cpuset.cpus.effective; sha256sum %s; "
                   "docker ps --format '{{.Names}}'" % shlex.quote(binary)))
    with open(os.path.join(args.out, "env.json"), "w") as f:
        json.dump(collect_env(binary), f, indent=1)
    jsonl = os.path.join(args.out, sc.name + ".jsonl")
    rows = []
    sub_names = []
    try:
        broker.start()
        sub_name, sub_cid, sub_names = start_subscribers(args, sc, broker)
        if sub_name is None:
            step = sc.make_step(rates[0]) if rates else {"label": "init", "payload": 128, "offered": 0}
            row = make_error_row(sc, step, args.step_secs, args.warm, "subscribers not connected")
            rows.append(row)
            with open(jsonl, "a") as f:
                f.write(json.dumps(row) + "\n")
            print(json.dumps(row), flush=True)
            return rows
        time.sleep(args.sub_wait)
        steps = sc.steps(rates)
        fails_in_a_row = 0
        for step in steps:
            print("[%s] step %s offered=%s" % (sc.name, step["label"], step["offered"]), flush=True)
            if not broker.alive():
                print("broker is not alive; stop", flush=True)
                break
            row = run_step(args, sc, broker, step, sub_name, sub_cid, sc.qos, sub_names)
            if args.profile:
                for ext in ("svg", "folded"):
                    src = broker.prof_prefix + "." + ext
                    if os.path.exists(src):
                        os.replace(src, os.path.join(args.out, "%s-%s.%s" % (sc.name, step["label"], ext)))
            rows.append(row)
            with open(jsonl, "a") as f:
                f.write(json.dumps(row) + "\n")
            print(json.dumps(row), flush=True)
            if row.get("error") in ("step timeout", "host overload"):
                print("guard stopped the step; stop scenario", flush=True)
                try:
                    broker.start()
                except Exception as e:
                    print("broker restart failed: %s" % e, flush=True)
                    break
                break
            if is_failing(row):
                fails_in_a_row += 1
            else:
                fails_in_a_row = 0
            if fails_in_a_row >= 2:
                print("two failing steps in a row; stop scenario", flush=True)
                break
    finally:
        try:
            if sub_names:
                shq("docker", "rm", "-f", "bs-pub", *sub_names)
            else:
                shq("docker", "rm", "-f", "bs-pub", "bs-sub")
        finally:
            try:
                broker.stop()
            finally:
                kill_tracked_probes()
    write_outputs(args, sc, rows)
    return rows


def write_outputs(args, sc, rows):
    if not rows:
        return
    keys = []
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    with open(os.path.join(args.out, sc.name + ".csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    best = pick_recommended(rows)
    ok = [r for r in rows if not r["generator_limited"] and (r.get("delivery_ratio") or 0.0) >= 0.9]
    summary = {
        "scenario": sc.name, "desc": sc.desc, "target_cpu_pct": 75,
        "recommended_step": best["step"] if best else None,
        "recommended_offered": best["offered"] if best else None,
        "recommended_in_rate": best["in_rate"] if best else None,
        "recommended_out_rate": best["out_rate"] if best else None,
        "recommended_cpu_pct": best["cpu_pct_of_4"] if best else None,
        "note": ("closest CPU to 75 percent among steps that are not generator-limited and deliver at "
                 "least 90 percent" if ok else "no clean step; the choice includes limited steps"),
        "steps": [{"step": r["step"], "offered": r["offered"], "in_rate": r["in_rate"], "out_rate": r["out_rate"],
                   "cpu_pct_of_4": r["cpu_pct_of_4"], "generator_limited": r["generator_limited"]} for r in rows],
    }
    with open(os.path.join(args.out, sc.name + ".summary.json"), "w") as f:
        json.dump(summary, f, indent=1)
    print("RECOMMENDED", json.dumps(summary["recommended_step"]), summary["recommended_cpu_pct"], flush=True)


def read_rows(path):
    """Read JSON lines from one file. Skip blank lines."""
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def write_all_summary(outdir):
    """Write one CSV row per scenario from all summary files."""
    path = os.path.join(outdir, "all-scenarios.csv")
    found = sorted(f for f in os.listdir(outdir) if f.endswith(".summary.json"))
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["scenario", "recommended_step", "offered", "in_rate", "out_rate",
                    "cpu_pct_of_4", "note"])
        for name in found:
            with open(os.path.join(outdir, name)) as sf:
                s = json.load(sf)
            w.writerow([s.get("scenario"), s.get("recommended_step"), s.get("recommended_offered"),
                        s.get("recommended_in_rate"), s.get("recommended_out_rate"),
                        s.get("recommended_cpu_pct"), s.get("note")])
    return path


def compare_runs(newdir, olddir):
    """Compare recommended out_rate per scenario. Return rows."""
    out = []
    for name in sorted(os.listdir(newdir)):
        if not name.endswith(".summary.json"):
            continue
        with open(os.path.join(newdir, name)) as f:
            new = json.load(f)
        old_path = os.path.join(olddir, name)
        old = {}
        if os.path.exists(old_path):
            with open(old_path) as f:
                old = json.load(f)
        n, o = new.get("recommended_out_rate"), old.get("recommended_out_rate")
        change = None
        if isinstance(n, (int, float)) and isinstance(o, (int, float)) and o:
            change = round((n - o) * 100.0 / o, 1)
        out.append({"scenario": new.get("scenario"), "old_out_rate": o, "new_out_rate": n,
                    "change_pct": change})
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list")
    r = sub.add_parser("run")
    r.add_argument("scenario", choices=sorted(SCENARIOS))
    r.add_argument("--binary", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--step-secs", type=int, default=300)
    r.add_argument("--warm", type=int, default=60, help="seconds at the start of each step that the averages skip")
    r.add_argument("--rates", help="comma list; inbound msg/s (S2: payload bytes)")
    r.add_argument("--sample-every", type=float, default=5.0)
    r.add_argument("--sub-wait", type=int, default=15)
    r.add_argument("--api-port", type=int, default=28083)
    r.add_argument("--no-probe", action="store_true")
    r.add_argument("--probe-pairs", type=int, default=20)
    r.add_argument("--probe-rate", type=float, default=50)
    r.add_argument("--bench-image", default=None, help="load generator image")
    r.add_argument("--bench-bin", default=None, help="load generator binary in the image")
    r.add_argument("--syscount", action="store_true", help="load libsyscount.so into the broker (diagnosis)")
    r.add_argument("--profile", type=int, nargs="?", const=499, default=0,
                   help="sampling profiler Hz; needs the diagnosis build of the broker")
    r.add_argument("--no-prof-window", action="store_true")
    s = sub.add_parser("summarize")
    s.add_argument("outdir")
    c = sub.add_parser("compare")
    c.add_argument("newdir")
    c.add_argument("olddir")
    c.add_argument("--out", required=True)
    args = ap.parse_args()
    if args.cmd == "list":
        for n, s in SCENARIOS.items():
            print(n, "|", s.desc, "| default rates:", s.default_rates)
        return
    if args.cmd == "summarize":
        print(write_all_summary(args.outdir))
        return
    if args.cmd == "compare":
        rows = compare_runs(args.newdir, args.olddir)
        with open(args.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["scenario", "old_out_rate", "new_out_rate", "change_pct"])
            w.writeheader()
            w.writerows(rows)
        for row in rows:
            print(json.dumps(row), flush=True)
        return
    ensure_process_group()
    install_cleanup_handlers()
    try:
        run_scenario(args)
    finally:
        cleanup_all()


if __name__ == "__main__":
    main()
