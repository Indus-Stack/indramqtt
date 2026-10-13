//! Example tests for the user documentation.
//!
//! This test reads each fenced block marked `sh test` or `toml test`
//! in `docs/`. It starts the broker with the configuration. It runs
//! the commands against the broker. It compares the output with the
//! `output` block that follows. A block marked `sh no-test` or
//! `toml no-test` states its reason in a comment. The test prints the
//! count of tested and untested blocks.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Credential seed for brokers that this test starts.
const DOCS_API_KEY: &str = "docs-example-key-0001";

/// One fenced block in a page.
struct Block {
    kind: BlockKind,
    marker: Option<String>,
    content: String,
    file: String,
    line: usize,
}

/// Block kinds that this test tracks.
#[derive(Clone, Copy, PartialEq, Eq)]
enum BlockKind {
    Shell,
    Toml,
    Output,
    Other,
}

/// A running broker plus its scratch directory.
struct Broker {
    child: Child,
    _work: tempfile::TempDir,
    cfg: PathBuf,
    mqtt: u16,
    api: u16,
}

impl Drop for Broker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Read each Markdown page under `docs/` in file-name order.
fn docs_pages(docs: &Path) -> Vec<PathBuf> {
    let mut pages = Vec::new();
    let mut dirs = vec![docs.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        let mut entries: Vec<PathBuf> = std::fs::read_dir(&dir)
            .unwrap_or_else(|error| panic!("read {}: {error}", dir.display()))
            .map(|entry| entry.expect("dir entry").path())
            .collect();
        entries.sort();
        for entry in entries {
            if entry.is_dir() {
                dirs.push(entry);
            } else if entry.extension().is_some_and(|ext| ext == "md") {
                pages.push(entry);
            }
        }
    }
    pages.sort();
    pages
}

/// Split one page into its fenced blocks.
fn parse_blocks(file: &Path, docs: &Path) -> Vec<Block> {
    let text = std::fs::read_to_string(file)
        .unwrap_or_else(|error| panic!("read {}: {error}", file.display()));
    let name = file
        .strip_prefix(docs)
        .unwrap_or(file)
        .display()
        .to_string();
    let mut blocks = Vec::new();
    let mut open: Option<(String, Option<String>, usize, Vec<String>)> = None;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("```") {
            if let Some((_, _, _, body)) = open.as_mut() {
                body.push(line.to_string());
            }
            continue;
        }
        match open.take() {
            None => {
                let mut words = trimmed[3..].split_whitespace();
                let lang = words.next().unwrap_or("").to_string();
                let marker = words.next().map(str::to_string);
                if words.next().is_some() {
                    panic!("{name}:{}: fence has a third word", index + 1);
                }
                open = Some((lang, marker, index + 1, Vec::new()));
            }
            Some((lang, marker, start, body)) => {
                let kind = match lang.as_str() {
                    "sh" | "bash" => BlockKind::Shell,
                    "toml" => BlockKind::Toml,
                    "output" => BlockKind::Output,
                    _ => BlockKind::Other,
                };
                blocks.push(Block {
                    kind,
                    marker,
                    content: body.join("\n"),
                    file: name.clone(),
                    line: start,
                });
            }
        }
    }
    if open.is_some() {
        panic!("{name}: fence never closes");
    }
    blocks
}

/// Check the markers. Every `sh` and `toml` block states `test` or
/// `no-test`. Every `no-test` block states its reason in a comment.
fn check_markers(blocks: &[Block], tested: &mut usize, untested: &mut usize) {
    for block in blocks {
        match block.kind {
            BlockKind::Other | BlockKind::Output => {}
            BlockKind::Shell | BlockKind::Toml => match block.marker.as_deref() {
                Some("test") => *tested += 1,
                Some("no-test") => {
                    *untested += 1;
                    let reason = block.content.lines().find(|line| !line.trim().is_empty());
                    match reason {
                        Some(first) if first.trim_start().starts_with('#') => {}
                        _ => panic!(
                            "{}:{}: no-test block states no reason in a comment",
                            block.file, block.line
                        ),
                    }
                }
                _ => panic!(
                    "{}:{}: sh/toml block lacks a test or no-test marker",
                    block.file, block.line
                ),
            },
        }
    }
}

/// Take a free loopback port. The caller binds it right away.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("free port");
    listener.local_addr().expect("port").port()
}

/// Move the default binds in a page config to free test ports.
fn rewrite_binds(text: &str, mqtt: u16, tls: u16, ws: u16, wss: u16, api: u16) -> String {
    let mut out = text.to_string();
    for (from, to) in [
        ("0.0.0.0:1883", mqtt),
        ("0.0.0.0:8883", tls),
        ("0.0.0.0:8083", ws),
        ("0.0.0.0:8084", wss),
        ("0.0.0.0:18083", api),
    ] {
        out = out.replace(from, &format!("127.0.0.1:{to}"));
    }
    out
}

/// Wait until the health endpoint answers `OK`.
fn wait_for_boot(api_port: u16) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", api_port)) {
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("timeout");
            let _ = stream.write_all(b"GET /healthz HTTP/1.0\r\nHost: docs\r\n\r\n");
            let mut body = String::new();
            if stream.read_to_string(&mut body).is_ok() && body.lines().last().unwrap_or("") == "OK"
            {
                return;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!("broker never answered on API port {api_port}");
}

/// Start the broker with one page configuration.
fn start_broker(toml: &str) -> Broker {
    let work = tempfile::TempDir::new().expect("temp dir");
    let cfg = work.path().join("cfg");
    std::fs::create_dir_all(cfg.join("conf.d")).expect("config dir");
    let ports = [
        free_port(),
        free_port(),
        free_port(),
        free_port(),
        free_port(),
    ];
    std::fs::write(
        cfg.join("indra.toml"),
        rewrite_binds(toml, ports[0], ports[1], ports[2], ports[3], ports[4]),
    )
    .expect("write config");
    std::fs::write(
        cfg.join("conf.d").join("99-docs-test.toml"),
        format!(
            "[node]\ndata_dir = \"{}\"\n",
            work.path().join("data").display()
        ),
    )
    .expect("write test fragment");
    let log = std::fs::File::create(work.path().join("broker.log")).expect("log file");
    let errors = log.try_clone().expect("log clone");
    let mut child = Command::new(env!("CARGO_BIN_EXE_indramqtt"))
        .arg("--config-dir")
        .arg(&cfg)
        .env("INDRA_API_KEYS", DOCS_API_KEY)
        .env("INDRA_API_KEY", DOCS_API_KEY)
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(errors))
        .spawn()
        .expect("start broker");
    wait_for_boot(ports[4]);
    if child.try_wait().expect("poll broker").is_some() {
        panic!(
            "broker exited at once; see {}/broker.log",
            work.path().display()
        );
    }
    Broker {
        child,
        _work: work,
        cfg,
        mqtt: ports[0],
        api: ports[4],
    }
}

/// Run one `sh test` block. Compare with the `output` block after it.
fn run_shell(block: &Block, expect: Option<&Block>, broker: Option<&Broker>) {
    let mut command = Command::new("bash");
    command
        .arg("-c")
        .arg(&block.content)
        .env("API_KEY", DOCS_API_KEY)
        .env("INDRAMQTT_BIN", env!("CARGO_BIN_EXE_indramqtt"))
        .env("INDRACLI_BIN", env!("CARGO_BIN_EXE_indra"))
        .env(
            "MQTT_PORT",
            broker.map_or_else(|| "1883".to_string(), |b| b.mqtt.to_string()),
        )
        .env(
            "API_PORT",
            broker.map_or_else(|| "18083".to_string(), |b| b.api.to_string()),
        );
    if let Some(running) = broker {
        command.env("DOCS_CFG_DIR", &running.cfg);
    }
    let result = command.output().expect("run shell");
    if !result.status.success() {
        panic!(
            "{}:{}: command failed ({}):\n{}",
            block.file,
            block.line,
            result.status,
            String::from_utf8_lossy(&result.stderr)
        );
    }
    let got = String::from_utf8_lossy(&result.stdout);
    match expect {
        Some(want) => {
            if got.trim_end() != want.content.trim_end() {
                panic!(
                    "{}:{}: output differs:\ngot:\n{}\nwant:\n{}",
                    block.file, block.line, got, want.content
                );
            }
        }
        None => panic!(
            "{}:{}: sh test block needs a following output block",
            block.file, block.line
        ),
    }
}

/// Run each tested block of one page.
fn run_page(blocks: &[Block]) {
    let mut broker: Option<Broker> = None;
    let mut index = 0;
    while index < blocks.len() {
        let block = &blocks[index];
        if block.kind == BlockKind::Toml && block.marker.as_deref() == Some("test") {
            broker = Some(start_broker(&block.content));
            index += 1;
            continue;
        }
        if block.kind == BlockKind::Shell && block.marker.as_deref() == Some("test") {
            let expect = match blocks.get(index + 1) {
                Some(other) if other.kind == BlockKind::Output => Some(other),
                _ => None,
            };
            run_shell(block, expect, broker.as_ref());
            index += 2;
            continue;
        }
        index += 1;
    }
}
#[test]
fn docs_examples() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let docs = root.join("../../docs");
    let docs = docs.canonicalize().expect("docs dir");
    let mut tested = 0usize;
    let mut untested = 0usize;
    let mut files = 0usize;
    for page in docs_pages(&docs) {
        let blocks = parse_blocks(&page, &docs);
        check_markers(&blocks, &mut tested, &mut untested);
        if blocks
            .iter()
            .any(|block| block.marker.as_deref() == Some("test"))
        {
            run_page(&blocks);
        }
        files += 1;
    }
    println!("DOCS_EXAMPLES tested={tested} untested={untested} files={files}");
    assert!(tested > 0, "docs hold no tested example");
}

/// Check the committed chart against the script output.
///
/// `docs/benchmarks/index.md` shows `assets/microbench.svg`. The script
/// `docs/_tools/render_charts.py` draws it from committed data. This test
/// reruns the script with `--check` and fails on any diff.
#[test]
fn microbench_chart_is_fresh() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let script = root.join("../../docs/_tools/render_charts.py");
    let result = Command::new("python3")
        .arg(&script)
        .arg("--check")
        .output()
        .expect("run render_charts.py --check");
    if !result.status.success() {
        panic!(
            "microbench.svg is stale:\n{}",
            String::from_utf8_lossy(&result.stdout)
        );
    }
}
