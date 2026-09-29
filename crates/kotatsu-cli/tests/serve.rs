//! `kotatsu serve` without a `kotatsud` binary on PATH.

use std::process::Command;

#[test]
fn missing_kotatsud_hint_does_not_need_a_checkout() {
    let empty = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("empty-path");
    std::fs::create_dir_all(&empty).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_kotatsu"))
        .arg("serve")
        .env("PATH", &empty)
        .output()
        .unwrap();
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("failed to exec `kotatsud`"), "{stderr}");
    assert!(stderr.contains("cargo install"), "{stderr}");
    // `--path crates/kotatsud` only works inside a clone of the repository.
    assert!(!stderr.contains("--path"), "{stderr}");
}
