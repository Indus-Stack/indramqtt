#[test]
fn pinned_git_deps_script_rejects_unpinned_and_accepts_tree() {
    // RQ-01: `tools/pinned_git_deps.sh` fails on a git dependency
    // without `rev` or `tag`, and passes on the tree.
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .ancestors()
        .nth(2)
        .expect("crate lives at <root>/crates/broker-rules")
        .to_path_buf();
    let script = repo_root.join("tools").join("pinned_git_deps.sh");
    assert!(script.is_file(), "missing {}", script.display());
    // Passes on the tree: no args, run from the root so the script
    // resolves the checkout the way the gates do.
    let ok = std::process::Command::new("bash")
        .arg(&script)
        .current_dir(&repo_root)
        .output()
        .expect("run pinned_git_deps.sh on the tree");
    assert!(
        ok.status.success(),
        "pinned check must pass on the tree: {}",
        String::from_utf8_lossy(&ok.stdout)
    );

    // Fails on a fixture Cargo.toml whose git dependency names
    // neither `rev` nor `tag`; passes once the pin is added.
    let scratch = std::env::temp_dir().join(format!(
        "broker-rules-pinned-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let fixture = scratch.join("Cargo.toml");
    let run_on_fixture = |contents: &str| {
        std::fs::create_dir_all(&scratch).expect("scratch dir");
        std::fs::write(&fixture, contents).expect("fixture writes");
        std::process::Command::new("bash")
            .arg(&script)
            .arg(&fixture)
            .output()
            .expect("run pinned_git_deps.sh on fixture")
    };
    let unpinned = run_on_fixture(
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\n\
         [dependencies]\nfoo = { git = \"https://example.com/foo.git\" }\n",
    );
    assert!(
        !unpinned.status.success(),
        "unpinned fixture must fail: {}",
        String::from_utf8_lossy(&unpinned.stdout)
    );
    let pinned_rev = run_on_fixture(
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\n\
         [dependencies]\nfoo = { git = \"https://example.com/foo.git\", rev = \"abc123\" }\n",
    );
    assert!(
        pinned_rev.status.success(),
        "rev-pinned fixture must pass: {}",
        String::from_utf8_lossy(&pinned_rev.stdout)
    );
    let pinned_tag = run_on_fixture(
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\n\
         [dependencies]\nfoo = { git = \"https://example.com/foo.git\", tag = \"v1.0\" }\n",
    );
    assert!(
        pinned_tag.status.success(),
        "tag-pinned fixture must pass: {}",
        String::from_utf8_lossy(&pinned_tag.stdout)
    );
    std::fs::remove_dir_all(&scratch).ok();
}
