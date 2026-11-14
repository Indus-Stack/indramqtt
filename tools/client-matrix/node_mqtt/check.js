#!/usr/bin/env node
// Client-library checklist for the Node `mqtt` package.
//
// The program speaks to a running broker through client sockets only.
// It prints one line for each check:
//
//     RESULT <check> <pass|fail|not-supported> <detail>
//
// Collect these lines and write the results table in results.md.
// This program imports the real `mqtt` package. Point NODE_PATH at
// the directory that holds it; see run.sh.
//
// It covers plain TCP and WebSocket where the library allows them.
// It covers MQTT 3.1.1 and MQTT 5.0.
"use strict";

const net = require("net");
const crypto = require("crypto");
const { spawn } = require("child_process");

let mqtt;
try {
  mqtt = require("mqtt");
} catch (err) {
  process.stderr.write("mqtt package is not importable: " + err.message + "\n");
  process.exit(3);
}

const HOST = process.env.MATRIX_HOST || "127.0.0.1";
const TCP_PORT = parseInt(process.env.MATRIX_TCP_PORT || "1883", 10);
const WS_PORT = parseInt(process.env.MATRIX_WS_PORT || "8083", 10);
const WS_PATH = process.env.MATRIX_WS_PATH || "/mqtt";
const API_BASE = process.env.MATRIX_API_BASE || "";
const API_KEY = process.env.MATRIX_API_KEY || "";
const USER = process.env.MATRIX_USER || "";
const PASSWORD = process.env.MATRIX_PASSWORD || "";
const PREFIX = process.env.MATRIX_PREFIX || "mx";
const RESTART_CMD = process.env.MATRIX_RESTART_CMD || "";

const CHECKS = [
  "v311-connect", "v5-connect", "qos0", "qos1", "qos2", "retained",
  "will", "session-resume", "shared", "payload-1mib", "auth",
  "keepalive", "restart",
];

let TRANSPORT = "tcp";

function emit(name, status, detail) {
  process.stdout.write("RESULT " + name + " " + status + " " + detail + "\n");
}

function tag(suffix) {
  return PREFIX + "-" + suffix + "-" + crypto.randomBytes(4).toString("hex");
}

function urlFor(transport) {
  if (transport === "ws") {
    return "ws://" + HOST + ":" + WS_PORT + WS_PATH;
  }
  return "mqtt://" + HOST + ":" + TCP_PORT;
}

// One client with promise helpers so checks stay readable.
class Conn {
  constructor(clientId, opts) {
    opts = opts || {};
    const protocolVersion = opts.protocolVersion || 4;
    const clean = opts.clean !== undefined ? opts.clean : true;
    const will = opts.will;
    const username = opts.username !== undefined ? opts.username : USER;
    const password = opts.password !== undefined ? opts.password : PASSWORD;
    const url = urlFor(TRANSPORT);
    const options = {
      clientId: clientId,
      protocolVersion: protocolVersion,
      reconnectPeriod: 0,
      connectTimeout: 8000,
      keepalive: opts.keepalive !== undefined ? opts.keepalive : 15,
    };
    if (protocolVersion === 5) {
      options.cleanStart = clean;
    } else {
      options.clean = clean;
    }
    if (username !== "") {
      options.username = username;
      options.password = password;
    }
    if (will) {
      options.will = {
        topic: will.topic,
        payload: will.payload,
        qos: will.qos,
        retain: will.retain,
      };
    }
    this.client = mqtt.connect(url, options);
    this.messages = [];
    this.code = null;
    this.sessionPresent = false;
    this.connectCount = 0;
    this.connectSettled = false;
    this.connectWaiter = null;
    this.messageWaiters = [];
    this.publishReject = null;
    this.lastError = null;
    this.client.on("connect", (connack) => {
      this.connectCount += 1;
      this.code = connack && typeof connack.returnCode === "number"
        ? connack.returnCode : 0;
      if (connack && typeof connack.sessionPresent === "boolean") {
        this.sessionPresent = connack.sessionPresent;
      }
      if (connack && connack.properties && connack.properties.reasonCode !== undefined) {
        this.code = connack.properties.reasonCode;
      }
      this.connectSettled = true;
      if (this.connectWaiter) {
        this.connectWaiter();
        this.connectWaiter = null;
      }
    });
    this.client.on("error", (err) => {
      // Latch the failure so start() sees it even when the event
      // fires before start() runs. A mid-check error rejects the
      // publish waiter so a closed socket cannot hang a check.
      this.connectSettled = true;
      this.lastError = err;
      if (this.connectWaiter) {
        this.connectWaiter();
        this.connectWaiter = null;
      }
      if (this.publishReject) {
        const reject = this.publishReject;
        this.publishReject = null;
        reject(err);
      }
    });
    this.client.on("message", (topic, payload, packet) => {
      const item = {
        topic: topic,
        payload: payload,
        retain: packet && packet.retain,
      };
      if (this.messageWaiters.length > 0) {
        const resolve = this.messageWaiters.shift();
        resolve(item);
      } else {
        this.messages.push(item);
      }
    });
  }

  start(timeoutMs) {
    timeoutMs = timeoutMs || 8000;
    if (this.connectSettled) {
      return Promise.resolve(this.code === 0);
    }
    return new Promise((resolve) => {
      const done = () => resolve(this.code === 0);
      const timer = setTimeout(() => {
        this.connectWaiter = null;
        resolve(false);
      }, timeoutMs);
      this.connectWaiter = () => {
        clearTimeout(timer);
        done();
      };
    });
  }

  publish(topic, payload, opts, timeoutMs) {
    timeoutMs = timeoutMs || 15000;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.publishReject = null;
        reject(new Error("publish not confirmed within " + timeoutMs + "ms"));
      }, timeoutMs);
      this.publishReject = (err) => {
        clearTimeout(timer);
        reject(err instanceof Error ? err : new Error(String(err)));
      };
      this.client.publish(topic, payload, opts, (err) => {
        clearTimeout(timer);
        this.publishReject = null;
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  subscribe(topic, opts, timeoutMs) {
    timeoutMs = timeoutMs || 10000;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.publishReject = null;
        reject(new Error("subscribe not confirmed within " + timeoutMs + "ms"));
      }, timeoutMs);
      this.publishReject = (err) => {
        clearTimeout(timer);
        reject(err instanceof Error ? err : new Error(String(err)));
      };
      this.client.subscribe(topic, opts, (err) => {
        clearTimeout(timer);
        this.publishReject = null;
        if (err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  }

  waitMessage(timeoutMs) {
    timeoutMs = timeoutMs || 10000;
    if (this.messages.length > 0) {
      return Promise.resolve(this.messages.shift());
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        const idx = this.messageWaiters.indexOf(resolver);
        if (idx >= 0) {
          this.messageWaiters.splice(idx, 1);
        }
        resolve(null);
      }, timeoutMs);
      const resolver = (item) => {
        clearTimeout(timer);
        resolve(item);
      };
      this.messageWaiters.push(resolver);
    });
  }

  stop(graceful) {
    if (graceful === undefined) {
      graceful = true;
    }
    try {
      if (graceful) {
        this.client.end(false);
      } else {
        this.client.end(true);
      }
    } catch (err) {
      // The socket may already be gone.
    }
  }
}

async function checkConnect(version) {
  const conn = new Conn(tag("v" + version), { protocolVersion: version });
  try {
    const ok = await conn.start();
    if (!ok) {
      throw new Error("no CONNACK within 8s (code " + conn.code + ")");
    }
    return "CONNACK " + conn.code;
  } finally {
    conn.stop();
  }
}

async function checkQos(qos) {
  const topic = tag("qos" + qos);
  const payload = "q" + qos + "-" + crypto.randomBytes(6).toString("hex");
  const sub = new Conn(tag("sub"));
  const pub = new Conn(tag("pub"));
  try {
    if (!(await sub.start())) {
      throw new Error("subscriber did not connect");
    }
    await sub.subscribe(topic, { qos: qos });
    await sleep(400);
    if (!(await pub.start())) {
      throw new Error("publisher did not connect");
    }
    await pub.publish(topic, payload, { qos: qos });
    const item = await sub.waitMessage(10000);
    if (!item) {
      throw new Error("no delivery at QoS " + qos);
    }
    if (item.payload.toString() !== payload) {
      throw new Error("payload mismatch");
    }
    return "delivered at QoS " + qos;
  } finally {
    pub.stop();
    sub.stop();
  }
}

async function checkRetained() {
  const topic = tag("retain");
  const payload = "retained-" + crypto.randomBytes(6).toString("hex");
  const pub = new Conn(tag("rpub"));
  try {
    if (!(await pub.start())) {
      throw new Error("publisher did not connect");
    }
    await pub.publish(topic, payload, { qos: 1, retain: true });
    await sleep(800);
  } finally {
    pub.stop();
  }
  const late = new Conn(tag("rsub"));
  try {
    if (!(await late.start())) {
      throw new Error("late subscriber did not connect");
    }
    await late.subscribe(topic, { qos: 1 });
    const item = await late.waitMessage(10000);
    if (!item) {
      throw new Error("late subscriber got no retained message");
    }
    if (item.payload.toString() !== payload) {
      throw new Error("retained payload mismatch");
    }
    if (!item.retain) {
      throw new Error("retained message arrived without the retain flag");
    }
    return "late subscriber got the retained message";
  } finally {
    late.stop();
  }
}

async function checkWill() {
  const topic = tag("will");
  const payload = "will-" + crypto.randomBytes(6).toString("hex");
  const obs = new Conn(tag("wobs"));
  try {
    if (!(await obs.start())) {
      throw new Error("observer did not connect");
    }
    await obs.subscribe(topic, { qos: 1 });
    await sleep(400);
    const child = spawn(process.execPath, [__filename, "--will-victim"], {
      env: Object.assign({}, process.env, {
        MATRIX_WILL_TOPIC: topic,
        MATRIX_WILL_PAYLOAD: payload,
        MATRIX_WILL_TRANSPORT: TRANSPORT,
      }),
      stdio: ["ignore", "pipe", "ignore"],
    });
    const ready = await readLine(child.stdout);
    if (ready !== "READY") {
      child.kill("SIGKILL");
      throw new Error("will victim did not connect (got " + JSON.stringify(ready) + ")");
    }
    await sleep(400);
    child.kill("SIGKILL");
    await new Promise((resolve) => {
      child.on("exit", resolve);
      setTimeout(resolve, 5000);
    });
    const item = await obs.waitMessage(10000);
    if (!item) {
      throw new Error("no will after the victim was killed");
    }
    if (item.payload.toString() !== payload) {
      throw new Error("will payload mismatch");
    }
    return "will message published on an abrupt close";
  } finally {
    obs.stop();
  }
}

function willVictim() {
  const topic = process.env.MATRIX_WILL_TOPIC;
  const payload = process.env.MATRIX_WILL_PAYLOAD;
  const transport = process.env.MATRIX_WILL_TRANSPORT || "tcp";
  TRANSPORT = transport;
  const conn = new Conn("mx-will-victim", {
    will: { topic: topic, payload: payload, qos: 1, retain: false },
  });
  return conn.start().then((ok) => {
    if (!ok) {
      process.stdout.write("FAILED\n");
      process.exit(1);
    }
    process.stdout.write("READY\n");
    // Hold the connection open until the parent kills this process.
    return new Promise(() => {});
  });
}

async function checkSessionResume() {
  const cid = tag("sess");
  const topic = tag("sessq");
  const payloads = [];
  for (let i = 0; i < 3; i++) {
    payloads.push("queued-" + i + "-" + crypto.randomBytes(3).toString("hex"));
  }
  const first = new Conn(cid, { clean: false });
  try {
    if (!(await first.start())) {
      throw new Error("first connect failed");
    }
    await first.subscribe(topic, { qos: 1 });
    await sleep(500);
  } finally {
    first.stop(false);
  }
  await sleep(400);
  const pub = new Conn(tag("spub"));
  try {
    if (!(await pub.start())) {
      throw new Error("publisher did not connect");
    }
    for (const payload of payloads) {
      await pub.publish(topic, payload, { qos: 1 });
    }
    await sleep(800);
  } finally {
    pub.stop();
  }
  const resumed = new Conn(cid, { clean: false });
  try {
    if (!(await resumed.start())) {
      throw new Error("resumed connect failed");
    }
    if (!resumed.sessionPresent) {
      throw new Error("sessionPresent was false on resume");
    }
    const got = new Set();
    const deadline = Date.now() + 10000;
    while (Date.now() < deadline && got.size < payloads.length) {
      const item = await resumed.waitMessage(1000);
      if (item) {
        got.add(item.payload.toString());
      }
    }
    if (got.size !== payloads.length) {
      throw new Error("queued messages missing: got " + got.size + " of " + payloads.length);
    }
    for (const payload of payloads) {
      if (!got.has(payload)) {
        throw new Error("a queued payload never arrived");
      }
    }
    return "session resumed and replayed " + got.size + " queued messages";
  } finally {
    resumed.stop();
  }
}

async function checkShared() {
  const group = tag("grp");
  const inner = tag("shared");
  const filter = "$share/" + group + "/" + inner;
  const count = 10;
  const first = new Conn(tag("sh1"));
  const second = new Conn(tag("sh2"));
  try {
    for (const conn of [first, second]) {
      if (!(await conn.start())) {
        throw new Error("shared subscriber did not connect");
      }
      await conn.subscribe(filter, { qos: 1 });
    }
    await sleep(600);
    const pub = new Conn(tag("shpub"));
    let seen = [];
    try {
      if (!(await pub.start())) {
        throw new Error("publisher did not connect");
      }
      for (let index = 0; index < count; index++) {
        await pub.publish(inner, "s" + index, { qos: 1 });
      }
      const deadline = Date.now() + 10000;
      while (Date.now() < deadline && seen.length < count) {
        for (const conn of [first, second]) {
          while (conn.messages.length > 0) {
            seen.push(conn.messages.shift().payload.toString());
          }
        }
        if (seen.length < count) {
          await sleep(100);
        }
      }
    } finally {
      pub.stop();
    }
    if (seen.length !== count) {
      throw new Error("shared group got " + seen.length + " of " + count + " messages");
    }
    if (new Set(seen).size !== count) {
      throw new Error("shared group delivered duplicates");
    }
    return "shared group delivered each of " + count + " messages once";
  } finally {
    first.stop();
    second.stop();
  }
}

async function checkPayload1mib() {
  const topic = tag("big");
  const payload = crypto.randomBytes(1024 * 1024);
  const sub = new Conn(tag("bigsub"));
  const pub = new Conn(tag("bigpub"));
  try {
    if (!(await sub.start())) {
      throw new Error("subscriber did not connect");
    }
    await sub.subscribe(topic, { qos: 1 });
    await sleep(500);
    if (!(await pub.start())) {
      throw new Error("publisher did not connect");
    }
    await pub.publish(topic, payload, { qos: 1 });
    const item = await sub.waitMessage(20000);
    if (!item) {
      throw new Error("1 MiB payload was not delivered");
    }
    if (item.payload.length !== payload.length) {
      throw new Error("1 MiB payload length " + item.payload.length);
    }
    return "1 MiB payload delivered";
  } finally {
    pub.stop();
    sub.stop();
  }
}

async function httpJson(method, path, body) {
  const url = new URL(API_BASE + path);
  const payload = body === undefined ? null : JSON.stringify(body);
  const headers = {
    Authorization: "Bearer " + API_KEY,
    "Content-Type": "application/json",
  };
  const res = await fetch(url, {
    method: method,
    headers: headers,
    body: payload,
  });
  const text = await res.text();
  return { status: res.status, body: text };
}

async function checkAuth() {
  if (!API_BASE || !API_KEY) {
    throw new Error("no API entry for user setup");
  }
  const user = tag("user");
  const good = "Good-pw-" + crypto.randomBytes(4).toString("hex") + "!";
  const created = await httpJson("POST", "/api/v1/auth/users", {
    username: user,
    password: good,
  });
  if (created.status !== 200 && created.status !== 201) {
    throw new Error("create user: HTTP " + created.status);
  }
  const accepted = new Conn(tag("au-ok"), { username: user, password: good });
  try {
    const ok = await accepted.start();
    if (!ok) {
      throw new Error("good credentials refused (code " + accepted.code + ")");
    }
  } finally {
    accepted.stop();
  }
  const refused = new Conn(tag("au-bad"), { username: user, password: "wrong-pw" });
  try {
    const ok = await refused.start();
    if (ok) {
      throw new Error("wrong password accepted");
    }
    const why = refused.code !== null
      ? "code " + refused.code
      : refused.lastError
        ? refused.lastError.message
        : "connection refused";
    return "good accepted, bad refused (" + why + ")";
  } finally {
    refused.stop();
  }
}

function rawConnect(username, password, keepalive) {
  // One raw socket observes the broker's own keep-alive timeout.
  return new Promise((resolve, reject) => {
    const sock = net.createConnection({ host: HOST, port: TCP_PORT }, () => {
      const field = (value) => {
        const buf = Buffer.from(value, "utf8");
        const len = Buffer.alloc(2);
        len.writeUInt16BE(buf.length);
        return Buffer.concat([len, buf]);
      };
      const body = Buffer.concat([
        Buffer.from([0x00, 0x04]),
        Buffer.from("MQTT", "utf8"),
        Buffer.from([0x04, 0xc2]),
        (() => {
          const ka = Buffer.alloc(2);
          ka.writeUInt16BE(keepalive);
          return ka;
        })(),
        field(tag("ka")),
        field(username),
        field(password),
      ]);
      const header = Buffer.from([0x10, body.length]);
      sock.write(Buffer.concat([header, body]));
    });
    sock.setTimeout(5000);
    sock.once("data", (data) => {
      sock.destroy();
      if (data.length < 4 || data[0] !== 0x20) {
        reject(new Error("bad CONNACK frame"));
        return;
      }
      resolve(data[3]);
    });
    sock.once("timeout", () => {
      sock.destroy();
      reject(new Error("CONNACK timeout"));
    });
    sock.once("error", (err) => {
      reject(err);
    });
  });
}

async function checkKeepalive() {
  const keepalive = 2;
  const code = await rawConnect(USER, PASSWORD, keepalive);
  if (code !== 0) {
    throw new Error("keepalive probe CONNACK " + code);
  }
  // Reopen and observe the idle close.
  const closedAfter = await new Promise((resolve, reject) => {
    const sock = net.createConnection({ host: HOST, port: TCP_PORT }, () => {
      const field = (value) => {
        const buf = Buffer.from(value, "utf8");
        const len = Buffer.alloc(2);
        len.writeUInt16BE(buf.length);
        return Buffer.concat([len, buf]);
      };
      const body = Buffer.concat([
        Buffer.from([0x00, 0x04]),
        Buffer.from("MQTT", "utf8"),
        Buffer.from([0x04, 0xc2]),
        (() => {
          const ka = Buffer.alloc(2);
          ka.writeUInt16BE(keepalive);
          return ka;
        })(),
        field(tag("ka2")),
        field(USER),
        field(PASSWORD),
      ]);
      sock.write(Buffer.concat([Buffer.from([0x10, body.length]), body]));
    });
    let gotAck = false;
    const start = Date.now();
    sock.setTimeout(keepalive * 3 * 1000 + 4000);
    sock.on("data", (data) => {
      if (!gotAck && data.length >= 4 && data[0] === 0x20) {
        gotAck = true;
        if (data[3] !== 0) {
          sock.destroy();
          reject(new Error("keepalive probe CONNACK " + data[3]));
        }
        return;
      }
      if (data.length > 0 && gotAck) {
        sock.destroy();
        reject(new Error("broker sent data on the idle connection"));
      }
    });
    sock.on("close", () => {
      resolve((Date.now() - start) / 1000);
    });
    sock.on("error", (err) => {
      reject(err);
    });
  });
  return "broker closed the idle connection after " + closedAfter.toFixed(1) + "s";
}

async function checkRestart() {
  if (!RESTART_CMD) {
    return null;
  }
  const first = new Conn(tag("rs"));
  try {
    if (!(await first.start())) {
      throw new Error("first connect failed");
    }
  } finally {
    first.stop();
  }
  const { execSync } = require("child_process");
  try {
    execSync(RESTART_CMD, { shell: "/bin/bash", timeout: 60000 });
  } catch (err) {
    throw new Error("restart command failed: " + err.message);
  }
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    const again = new Conn(tag("rs2"));
    try {
      if ((await again.start(3000)) && again.code === 0) {
        return "reconnect after restart works";
      }
    } finally {
      again.stop();
    }
    await sleep(1000);
  }
  throw new Error("no reconnect within 30s of the restart");
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function readLine(stream) {
  return new Promise((resolve) => {
    let buf = "";
    const onData = (chunk) => {
      buf += chunk.toString();
      const nl = buf.indexOf("\n");
      if (nl >= 0) {
        stream.removeListener("data", onData);
        resolve(buf.slice(0, nl).trim());
      }
    };
    stream.on("data", onData);
    stream.once("end", () => {
      resolve(buf.trim());
    });
    setTimeout(() => {
      stream.removeListener("data", onData);
      resolve(buf.trim());
    }, 8000);
  });
}

const FUNCS = {
  "v311-connect": () => checkConnect(4),
  "v5-connect": () => checkConnect(5),
  qos0: () => checkQos(0),
  qos1: () => checkQos(1),
  qos2: () => checkQos(2),
  retained: checkRetained,
  will: checkWill,
  "session-resume": checkSessionResume,
  shared: checkShared,
  "payload-1mib": checkPayload1mib,
  auth: checkAuth,
  keepalive: checkKeepalive,
  restart: checkRestart,
};

async function main() {
  if (process.argv[2] === "--will-victim") {
    await willVictim();
    return;
  }
  TRANSPORT = process.argv[2] || "tcp";
  const wanted = process.argv.slice(3);
  for (const name of CHECKS) {
    if (wanted.length > 0 && wanted.indexOf(name) < 0) {
      continue;
    }
    try {
      const detail = await FUNCS[name]();
      if (detail === null) {
        emit(name, "not-supported", "no restart command in this run");
      } else {
        emit(name, "pass", detail);
      }
    } catch (err) {
      emit(name, "fail", (err && err.name ? err.name : "Error") + ": " + err.message);
    }
  }
}

main().then(() => {
  process.exit(0);
}).catch((err) => {
  process.stderr.write(String(err && err.stack ? err.stack : err) + "\n");
  process.exit(1);
});
