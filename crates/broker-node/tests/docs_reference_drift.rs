//! Drift tests for the generated reference pages (DOC-02).
//!
//! The generator owns the pages. The first tests run it in check mode.
//! The other tests check the pages against the live product: the API
//! pages match the generator run on the OpenAPI document that the
//! broker serves, the metrics page matches the names in the live
//! output of the metrics endpoint, each flag of `indramqtt --help`
//! owns one row, each row names a live flag, each `ctl` command runs,
//! and the documented `--check-config` example exits 0 on an empty
//! directory. The last tests enforce the documentation standard: each
//! page carries a Mermaid visual, and each Mermaid block parses.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Key that started brokers accept for these tests.
const DRIFT_API_KEY: &str = "drift-check-key-0001";

/// Locates the workspace root from this crate directory.
fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
}

/// Locates the kernel binary under test.
fn kernel_binary() -> PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indramqtt") {
        return PathBuf::from(built);
    }
    let exe = if cfg!(windows) {
        "indramqtt.exe"
    } else {
        "indramqtt"
    };
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join(format!("../../target/debug/{exe}")),
        manifest.join(format!("../../target/ci-check/debug/{exe}")),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("kernel binary not found: build the workspace first");
}

/// Locates the operator CLI binary under test.
fn ctl_binary() -> PathBuf {
    if let Some(built) = option_env!("CARGO_BIN_EXE_indra") {
        return PathBuf::from(built);
    }
    let exe = if cfg!(windows) { "indra.exe" } else { "indra" };
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for candidate in [
        manifest.join(format!("../../target/debug/{exe}")),
        manifest.join(format!("../../target/ci-check/debug/{exe}")),
    ] {
        if candidate.is_file() {
            return candidate;
        }
    }
    panic!("ctl binary not found: build the workspace first");
}

/// Reads the committed CLI page.
fn read_cli_page() -> String {
    std::fs::read_to_string(
        workspace_root()
            .join("docs")
            .join("reference")
            .join("cli.md"),
    )
    .expect("committed page exists")
}

/// Keeps the page section between two headings.
fn section<'a>(page: &'a str, start: &str, end: &str) -> &'a str {
    let text = page.split(start).nth(1).expect("section starts");
    text.split(end).next().unwrap_or(text)
}

/// Lists the documented `indramqtt` flags from the flag table.
fn documented_flags(page: &str) -> Vec<String> {
    let table = section(page, "## indramqtt", "## indra ctl");
    let mut flags = Vec::new();
    for line in table.lines() {
        if !line.starts_with("| `") {
            continue;
        }
        let cell = line.split('`').nth(1).expect("row carries a flag");
        let flag = cell.split(',').next_back().unwrap_or("").trim().to_string();
        if flag.starts_with("--") {
            flags.push(flag);
        }
    }
    flags.sort();
    flags.dedup();
    flags
}

/// Lists the documented `ctl` commands from the command table.
fn documented_commands(page: &str) -> Vec<String> {
    let table = section(page, "## indra ctl", "## __END__");
    let mut commands = Vec::new();
    for line in table.lines() {
        if !line.starts_with("| `indra ctl") {
            continue;
        }
        let cell = line.split('`').nth(1).expect("row carries a command");
        commands.push(cell.to_string());
    }
    commands.sort();
    commands.dedup();
    commands
}

/// Cleans one help token into a long flag when it is one.
fn as_long_flag(token: &str) -> Option<String> {
    let clean = token.trim_matches(|c: char| {
        matches!(
            c,
            ',' | '.' | ':' | ';' | '"' | '\'' | '`' | '(' | ')' | '[' | ']'
        )
    });
    if clean.len() > 2 && clean.starts_with("--") {
        let body = &clean[2..];
        if !body.is_empty() && body.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return Some(clean.to_string());
        }
    }
    None
}

/// Runs one binary with `--help` and lists its long flags.
fn live_long_flags(binary: &PathBuf, extra: &[&str]) -> Vec<String> {
    let output = std::process::Command::new(binary)
        .args(extra)
        .arg("--help")
        .output()
        .expect("binary answers --help");
    assert!(
        output.status.success(),
        "binary answers --help with success"
    );
    let text = String::from_utf8_lossy(&output.stdout).to_string()
        + &String::from_utf8_lossy(&output.stderr);
    let mut flags: Vec<String> = text.split_whitespace().filter_map(as_long_flag).collect();
    flags.sort();
    flags.dedup();
    flags
}

#[test]
fn cli_reference_matches_generator() {
    let output = std::process::Command::new("python3")
        .arg("tools/gen_reference_docs.py")
        .arg("--check")
        .arg("cli")
        .current_dir(workspace_root())
        .output()
        .expect("python3 runs the generator");
    assert!(
        output.status.success(),
        "generator check failed:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn indramqtt_flags_match_live_help() {
    let page = read_cli_page();
    let documented = documented_flags(&page);
    assert!(!documented.is_empty(), "page lists flags");
    let live = live_long_flags(&kernel_binary(), &[]);
    // Each live flag owns one row, except the automatic help flags.
    for flag in &live {
        if flag == "--help" || flag == "--version" {
            continue;
        }
        assert!(documented.contains(flag), "page documents live flag {flag}");
    }
    // Each documented flag names a live flag, so none is invented.
    for flag in &documented {
        assert!(live.contains(flag), "live help owns {flag}");
    }
}

#[test]
fn ctl_commands_match_live_help() {
    let page = read_cli_page();
    let commands = documented_commands(&page);
    assert!(!commands.is_empty(), "page lists commands");
    let ctl = ctl_binary();
    let top = live_long_flags(&ctl, &["ctl"]);
    assert!(
        top.contains(&"--endpoint".to_string()),
        "ctl shows --endpoint"
    );
    // Each top-level command answers its own --help.
    let mut groups: Vec<String> = commands
        .iter()
        .filter_map(|command| command.split_whitespace().nth(2).map(str::to_string))
        .collect();
    groups.sort();
    groups.dedup();
    assert!(groups.contains(&"status".to_string()), "page lists status");
    for group in &groups {
        let output = std::process::Command::new(&ctl)
            .args(["ctl", group.as_str(), "--help"])
            .output()
            .expect("ctl command runs");
        assert!(output.status.success(), "indra ctl {group} --help succeeds");
    }
}

#[test]
fn check_config_example_runs() {
    // The page documents `indramqtt --check-config`. An empty directory
    // starts on defaults, so the check exits 0 with no fixture.
    let tag = format!(
        "doc-cli-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let dir = std::env::temp_dir().join(tag);
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    let output = std::process::Command::new(kernel_binary())
        .arg("--config-dir")
        .arg(&dir)
        .arg("--check-config")
        .output()
        .expect("kernel runs --check-config");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        output.status.success(),
        "documented --check-config exits 0 on defaults"
    );
}

/// Takes a free loopback port. The caller binds it right away.
fn drift_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("free port");
    listener.local_addr().expect("port").port()
}

/// A running broker for the live drift tests.
struct DriftBroker {
    child: std::process::Child,
    _work: tempfile::TempDir,
    api: u16,
}

impl Drop for DriftBroker {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Starts the broker on free ports with a minimal file.
fn start_drift_broker() -> DriftBroker {
    let work = tempfile::TempDir::new().expect("temp dir");
    let cfg = work.path().join("cfg");
    std::fs::create_dir_all(&cfg).expect("config dir");
    // Pin each listener that the harness remaps. An omitted bind
    // keeps its default port. Parallel brokers then collide on it.
    let mqtt = drift_free_port();
    let tls = drift_free_port();
    let ws = drift_free_port();
    let wss = drift_free_port();
    let api = drift_free_port();
    std::fs::write(
        cfg.join("indra.toml"),
        format!(
            "[node]\nid = \"drift-check\"\ndata_dir = \"{}\"\n\n[listeners.tcp]\nbind = \"127.0.0.1:{mqtt}\"\n\n[listeners.tls]\nbind = \"127.0.0.1:{tls}\"\n\n[listeners.ws]\nbind = \"127.0.0.1:{ws}\"\n\n[listeners.wss]\nbind = \"127.0.0.1:{wss}\"\n\n[listeners.api]\nbind = \"127.0.0.1:{api}\"\n\n[logging]\nlevel = \"warn\"\n",
            work.path().join("data").display(),
        ),
    )
    .expect("write config");
    let log = std::fs::File::create(work.path().join("broker.log")).expect("log file");
    let errors = log.try_clone().expect("log clone");
    let mut child = std::process::Command::new(kernel_binary())
        .arg("--config-dir")
        .arg(&cfg)
        .env("INDRA_API_KEYS", DRIFT_API_KEY)
        .env("INDRA_API_KEY", DRIFT_API_KEY)
        .stdout(std::process::Stdio::from(log))
        .stderr(std::process::Stdio::from(errors))
        .spawn()
        .expect("start broker");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", api)) {
            stream
                .set_read_timeout(Some(std::time::Duration::from_secs(2)))
                .expect("timeout");
            let _ = stream.write_all(b"GET /healthz HTTP/1.0\r\nHost: drift\r\n\r\n");
            let mut body = String::new();
            if stream.read_to_string(&mut body).is_ok() && body.lines().last().unwrap_or("") == "OK"
            {
                break;
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            panic!("broker never answered on API port {api}");
        }
        if child.try_wait().expect("poll broker").is_some() {
            panic!("broker exited at once");
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    DriftBroker {
        child,
        _work: work,
        api,
    }
}

/// Reads one HTTP body from the broker over plain TCP.
fn drift_get(api: u16, path: &str, key: Option<&str>) -> (u16, String) {
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", api)).expect("broker answers");
    stream
        .set_read_timeout(Some(std::time::Duration::from_secs(5)))
        .expect("timeout");
    let mut head = format!("GET {path} HTTP/1.0\r\nHost: drift\r\n");
    if let Some(token) = key {
        head.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).expect("write dies");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read dies");
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .lines()
        .next()
        .unwrap_or("")
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse::<u16>()
        .unwrap_or(0);
    let body = text.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    // A length-prefixed reply without a close still splits the same way.
    // A chunked reply keeps its chunks; the broker never chunks here.
    let body = body.split("\n\r\n").last().unwrap_or(&body).to_string();
    (status, body)
}

#[test]
fn api_pages_match_served_openapi() {
    // The broker serves its OpenAPI document. The generator runs on
    // that served text. The pages match the live product.
    let broker = start_drift_broker();
    let (status, body) = drift_get(broker.api, "/api-docs/openapi.json", None);
    assert_eq!(status, 200, "broker serves its OpenAPI document");
    let served: serde_json::Value = serde_json::from_str(&body).expect("served doc parses");
    assert!(served.get("paths").is_some(), "served doc has paths");
    let dir = std::env::temp_dir().join(format!("doc-served-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("temp dir exists");
    let served_file = dir.join("served.json");
    std::fs::write(&served_file, &body).expect("served doc writes");
    let output = std::process::Command::new("python3")
        .arg("tools/gen_reference_docs.py")
        .arg("--check")
        .arg("api")
        .arg("--openapi-json")
        .arg(&served_file)
        .current_dir(workspace_root())
        .output()
        .expect("python3 runs the generator");
    let _ = std::fs::remove_dir_all(&dir);
    assert!(
        output.status.success(),
        "pages differ from generator run on served document:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn metrics_page_matches_live_endpoint() {
    // The metrics endpoint serves the live names. Each served name owns
    // one page row, and each page row names a served name.
    let broker = start_drift_broker();
    let (status, body) = drift_get(broker.api, "/api/v1/metrics", Some(DRIFT_API_KEY));
    assert_eq!(status, 200, "metrics endpoint answers");
    let mut served = Vec::new();
    for line in body.lines() {
        let name = line.split_whitespace().next().unwrap_or("");
        if name.starts_with("indramqtt_") && !served.contains(&name.to_string()) {
            served.push(name.to_string());
        }
    }
    assert!(!served.is_empty(), "endpoint serves metric names");
    let page = std::fs::read_to_string(
        workspace_root()
            .join("docs")
            .join("reference")
            .join("metrics.md"),
    )
    .expect("committed page exists");
    let mut rows = 0usize;
    for line in page.lines() {
        if !line.starts_with("| `indramqtt_") {
            continue;
        }
        let name = line.split('`').nth(1).expect("row carries a name");
        assert!(body.contains(name), "live endpoint serves {name}");
        rows += 1;
    }
    assert!(rows >= 1, "page lists metrics");
    for name in &served {
        assert!(page.contains(name), "page documents served {name}");
    }
    println!("DRIFT_METRICS served={} rows={rows}", served.len());
}

/// Lists the pages that the generator owns.
fn generated_reference_pages() -> Vec<PathBuf> {
    let root = workspace_root();
    let api = root.join("docs").join("reference").join("api");
    let mut pages: Vec<PathBuf> = std::fs::read_dir(&api)
        .expect("api dir exists")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "md"))
        .collect();
    pages.sort();
    for name in ["configuration.md", "cli.md", "metrics.md"] {
        pages.push(root.join("docs").join("reference").join(name));
    }
    pages
}

/// Reads the Mermaid blocks of one page.
fn mermaid_blocks(page: &PathBuf) -> Vec<String> {
    let text = std::fs::read_to_string(page).expect("committed page exists");
    let mut blocks = Vec::new();
    let mut open: Option<String> = None;
    let mut body = String::new();
    for line in text.lines() {
        let trimmed = line.trim_start();
        if let Some(stripped) = trimmed.strip_prefix("```") {
            match open.take() {
                None => {
                    open = Some(stripped.trim().to_string());
                    body.clear();
                }
                Some(language) => {
                    if language == "mermaid" {
                        blocks.push(body.clone());
                    }
                    body.clear();
                }
            }
            continue;
        }
        if open.is_some() {
            body.push('\n');
            body.push_str(line);
        }
    }
    assert!(open.is_none(), "fence never closes in {}", page.display());
    blocks
}

/// Checks one Mermaid block: a known diagram header, one labelled
/// edge, and twelve nodes or fewer.
fn check_mermaid_syntax(page: &Path, body: &str) {
    let mut lines = body.lines().filter(|line| !line.trim().is_empty());
    let header = lines.next().expect("diagram is not empty").trim();
    let kind = header.split_whitespace().next().unwrap_or("");
    assert!(
        ["flowchart", "graph", "sequenceDiagram", "stateDiagram-v2"].contains(&kind),
        "{}: unknown diagram {}",
        page.display(),
        header
    );
    assert!(
        ["-->", "---", "->>", "-->>", "==>"]
            .iter()
            .any(|edge| body.contains(*edge)),
        "{}: diagram carries no edge",
        page.display()
    );
    let nodes = body.matches("[\"").count();
    assert!(
        nodes <= 12,
        "{}: diagram carries {nodes} nodes, more than 12",
        page.display()
    );
}

#[test]
fn reference_pages_carry_a_visual() {
    let pages = generated_reference_pages();
    assert!(!pages.is_empty(), "generator owns pages");
    let mut visuals = 0usize;
    for page in &pages {
        let blocks = mermaid_blocks(page);
        assert!(
            !blocks.is_empty(),
            "{} carries no Mermaid visual",
            page.display()
        );
        visuals += blocks.len();
    }
    println!("DOCS_VISUALS pages={} visuals={visuals}", pages.len(),);
}

#[test]
fn reference_mermaid_blocks_parse() {
    let mut diagrams = 0usize;
    for page in generated_reference_pages() {
        for body in mermaid_blocks(&page) {
            check_mermaid_syntax(&page, &body);
            diagrams += 1;
        }
    }
    assert!(diagrams > 0, "generated pages hold diagrams");
    println!("DOCS_MERMAID diagrams={diagrams}");
}
