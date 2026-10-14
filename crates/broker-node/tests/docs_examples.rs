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

/// Free loopback ports for one started broker.
struct Binds {
    mqtt: u16,
    tls: u16,
    ws: u16,
    wss: u16,
    api: u16,
    cluster: u16,
    gossip: u16,
    quic: u16,
}

/// Move the default binds in a page config to free test ports.
/// Remap cluster binds to free ports. Parallel brokers share the host.
/// Fixed ports collide.
fn rewrite_binds(text: &str, binds: Binds) -> String {
    let mut out = text.to_string();
    for (from, to) in [
        ("0.0.0.0:1883", binds.mqtt),
        ("0.0.0.0:8883", binds.tls),
        ("0.0.0.0:8083", binds.ws),
        ("0.0.0.0:8084", binds.wss),
        ("0.0.0.0:18083", binds.api),
        ("127.0.0.1:19883", binds.cluster),
        ("0.0.0.0:7946", binds.gossip),
        ("0.0.0.0:9092", binds.quic),
    ] {
        out = out.replace(from, &format!("127.0.0.1:{to}"));
    }
    out
}

/// Wait until the health endpoint answers `OK`.
/// Wait 30 seconds for boot. Parallel gates load the host.
/// Slow boot still passes.
fn wait_for_boot(api_port: u16) {
    let deadline = Instant::now() + Duration::from_secs(30);
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
        free_port(),
        free_port(),
        free_port(),
    ];
    std::fs::write(
        cfg.join("indra.toml"),
        rewrite_binds(
            toml,
            Binds {
                mqtt: ports[0],
                tls: ports[1],
                ws: ports[2],
                wss: ports[3],
                api: ports[4],
                cluster: ports[5],
                gossip: ports[6],
                quic: ports[7],
            },
        ),
    )
    .expect("write config");
    // Page configs omit idle listeners, so fixed default ports collide.
    // Pin idle and cluster binds to free ports here.
    std::fs::write(
        cfg.join("conf.d").join("99-docs-test.toml"),
        format!(
            "[node]\ndata_dir = \"{}\"\n\
             [listeners.tcp]\nbind = \"127.0.0.1:{}\"\n\
             [listeners.tls]\nbind = \"127.0.0.1:{}\"\n\
             [listeners.ws]\nbind = \"127.0.0.1:{}\"\n\
             [listeners.wss]\nbind = \"127.0.0.1:{}\"\n\
             [listeners.api]\nbind = \"127.0.0.1:{}\"\n\
             [cluster]\nbind = \"127.0.0.1:{}\"\ngossip_bind = \"127.0.0.1:{}\"\nquic_bind = \"127.0.0.1:{}\"\n",
            work.path().join("data").display(),
            ports[0],
            ports[1],
            ports[2],
            ports[3],
            ports[4],
            ports[5],
            ports[6],
            ports[7],
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
/// Return the real output. The caller stores it in the capture file.
fn run_shell(block: &Block, expect: Option<&Block>, broker: Option<&Broker>) -> String {
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
    got.trim_end().to_string()
}

/// Format one captured output for the capture table.
fn escape_cell(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('|', "\\|")
        .replace('`', "\\`")
        .replace('\n', "<br>")
}

/// Write the capture file from the real outputs of this run.
fn write_captured_outputs(docs: &Path, captures: &[(String, usize, String)]) {
    let mut out = vec![
        "# Captured outputs (DOC-02)".to_string(),
        String::new(),
        "```mermaid".to_string(),
        "flowchart LR".to_string(),
        "    H[\"docs harness\"] -- \"run block\" --> B[\"broker\"]".to_string(),
        "    B -- \"compare output\" --> O[\"output block\"]".to_string(),
        "```".to_string(),
        String::new(),
        "The docs harness writes this file on each run. It runs each `sh test` block against a started broker."
            .to_string(),
        "It stores the real output it compares with the `output` block. The gates record the run result."
            .to_string(),
        String::new(),
        "## Captured blocks".to_string(),
        String::new(),
        "The list holds one row per `sh test` block. The output column holds the real output.".to_string(),
        String::new(),
        "| Page | Line | Output |".to_string(),
        "|---|---|---|".to_string(),
    ];
    for (file, line, output) in captures {
        out.push(format!("| `{file}` | {line} | `{}` |", escape_cell(output)));
    }
    out.push(String::new());
    out.push(format!(
        "The run captured {} blocks. Each output matches its page.",
        captures.len()
    ));
    out.push(String::new());
    std::fs::write(
        docs.join("_verification").join("captured-outputs.md"),
        out.join("\n"),
    )
    .expect("write capture file");
}

/// Run each tested block of one page. Return each real output.
fn run_page(blocks: &[Block]) -> Vec<(String, usize, String)> {
    // The generated API pages place the example flow before the
    // boot file. Boot the broker from the first file block first,
    // so the flow runs against a started broker.
    let mut broker: Option<Broker> = None;
    let mut booted: Option<String> = None;
    if let Some(first) = blocks
        .iter()
        .find(|block| block.kind == BlockKind::Toml && block.marker.as_deref() == Some("test"))
    {
        booted = Some(first.content.clone());
        broker = Some(start_broker(&first.content));
    }
    let mut index = 0;
    let mut captures = Vec::new();
    while index < blocks.len() {
        let block = &blocks[index];
        if block.kind == BlockKind::Toml && block.marker.as_deref() == Some("test") {
            if booted.as_deref() != Some(block.content.as_str()) {
                broker = Some(start_broker(&block.content));
                booted = Some(block.content.clone());
            }
            index += 1;
            continue;
        }
        if block.kind == BlockKind::Shell && block.marker.as_deref() == Some("test") {
            let expect = match blocks.get(index + 1) {
                Some(other) if other.kind == BlockKind::Output => Some(other),
                _ => None,
            };
            let got = run_shell(block, expect, broker.as_ref());
            captures.push((block.file.clone(), block.line, got));
            index += 2;
            continue;
        }
        index += 1;
    }
    captures
}
#[test]
fn docs_examples() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let docs = root.join("../../docs");
    let docs = docs.canonicalize().expect("docs dir");
    let mut tested = 0usize;
    let mut untested = 0usize;
    let mut files = 0usize;
    let mut all_captures: Vec<(String, usize, String)> = Vec::new();
    for page in docs_pages(&docs) {
        let blocks = parse_blocks(&page, &docs);
        check_markers(&blocks, &mut tested, &mut untested);
        if blocks
            .iter()
            .any(|block| block.marker.as_deref() == Some("test"))
        {
            all_captures.extend(run_page(&blocks));
        }
        files += 1;
    }
    write_captured_outputs(&docs, &all_captures);
    println!("DOCS_EXAMPLES tested={tested} untested={untested} files={files}");
    assert!(tested > 0, "docs hold no tested example");
}

/// Check that each page carries a visual with valid syntax.
///
/// Each page under `docs/` holds at least one `mermaid` diagram or
/// one chart image. Each `mermaid` block opens with a known diagram.
/// A page with no visual fails.
#[test]
fn docs_visuals() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let docs = root.join("../../docs");
    let docs = docs.canonicalize().expect("docs dir");
    let mut files = 0usize;
    for page in docs_pages(&docs) {
        let text = std::fs::read_to_string(&page)
            .unwrap_or_else(|error| panic!("read {}: {error}", page.display()));
        let name = page
            .strip_prefix(&docs)
            .unwrap_or(&page)
            .display()
            .to_string();
        let mut diagrams: Vec<(usize, Vec<String>)> = Vec::new();
        let mut open: Option<(usize, Vec<String>)> = None;
        for (index, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if !trimmed.starts_with("```") {
                if let Some((_, body)) = open.as_mut() {
                    body.push(line.to_string());
                }
                continue;
            }
            if let Some((start, body)) = open.take() {
                diagrams.push((start, body));
            } else if let Some(rest) = trimmed.strip_prefix("```mermaid") {
                let mut body = Vec::new();
                if !rest.trim().is_empty() {
                    body.push(rest.trim().to_string());
                }
                open = Some((index + 1, body));
            }
        }
        if open.is_some() {
            panic!("{name}: mermaid fence never closes");
        }
        for (start, body) in &diagrams {
            let first = body
                .iter()
                .map(|line| line.trim())
                .find(|line| !line.is_empty())
                .unwrap_or("");
            if !(first.starts_with("flowchart")
                || first.starts_with("graph ")
                || first.starts_with("sequenceDiagram")
                || first.starts_with("stateDiagram"))
            {
                panic!("{name}:{start}: unknown mermaid diagram `{first}`");
            }
        }
        let has_chart = text.contains(".svg");
        if diagrams.is_empty() && !has_chart {
            panic!("{name}: page carries no visual");
        }
        files += 1;
    }
    println!("DOCS_VISUALS files={files}");
    assert!(files > 0, "docs hold no pages");
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
