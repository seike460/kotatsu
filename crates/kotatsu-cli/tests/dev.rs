//! `kotatsu dev` as a process: shutdown signals and the app's
//! `/terminate` hook.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::Duration;

fn terminate_path() -> String {
    format!("{}/terminate", kotatsu::HOOK_PATH_PREFIX)
}

/// A local app that answers every request 200 except `/terminate`,
/// which gets `terminate_status`. Each request path is reported before
/// the response is written.
fn app(terminate_status: u16) -> (String, mpsc::Receiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for conn in listener.incoming() {
            let Ok(mut conn) = conn else { continue };
            let Some(path) = read_request(&mut conn) else {
                continue;
            };
            let status = if path == terminate_path() {
                terminate_status
            } else {
                200
            };
            let _ = tx.send(path);
            let _ = conn.write_all(
                format!("HTTP/1.1 {status} X\r\ncontent-length: 0\r\nconnection: close\r\n\r\n")
                    .as_bytes(),
            );
        }
    });
    (url, rx)
}

/// Reads one request, body included, and returns its path.
fn read_request(conn: &mut TcpStream) -> Option<String> {
    let mut r = BufReader::new(conn);
    let mut line = String::new();
    r.read_line(&mut line).ok()?;
    let path = line.split(' ').nth(1)?.to_owned();
    let mut len = 0;
    loop {
        let mut h = String::new();
        if r.read_line(&mut h).ok()? == 0 || h == "\r\n" {
            break;
        }
        if let Some((k, v)) = h.split_once(':')
            && k.eq_ignore_ascii_case("content-length")
        {
            len = v.trim().parse().ok()?;
        }
    }
    r.read_exact(&mut vec![0; len]).ok()?;
    Some(path)
}

/// Starts `kotatsu dev` in front of `app_url` and returns once it logs
/// "emulator ready".
fn start_dev(app_url: &str) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kotatsu"))
        .args(["dev", "--app-url", app_url])
        .env("RUST_LOG", "kotatsu=info")
        .env("NO_COLOR", "1")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (ready_tx, ready_rx) = mpsc::channel();
    // Keeps draining stdout so the child never writes to a closed pipe.
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if line.contains("emulator ready") {
                let _ = ready_tx.send(());
            }
        }
    });
    if ready_rx.recv_timeout(Duration::from_secs(30)).is_err() {
        let _ = child.kill();
        let (_, stderr) = finish(child);
        panic!("kotatsu dev never became ready: {stderr}");
    }
    child
}

fn kill(pid: u32, signal: &str) {
    let sent = Command::new("kill")
        .args([format!("-{signal}"), pid.to_string()])
        .status()
        .unwrap();
    assert!(sent.success());
}

/// Waits for `child` to exit and returns its status and stderr.
fn finish(child: Child) -> (ExitStatus, String) {
    let pid = child.id();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    let Ok(out) = rx.recv_timeout(Duration::from_secs(20)) else {
        kill(pid, "KILL");
        panic!("kotatsu dev did not exit");
    };
    let out = out.unwrap();
    (
        out.status,
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

fn assert_clean_shutdown_on(signal: &str) {
    let (url, paths) = app(200);
    let child = start_dev(&url);
    kill(child.id(), signal);
    let (status, stderr) = finish(child);
    assert!(status.success(), "SIG{signal}: {status}: {stderr}");
    assert!(
        paths.try_iter().any(|p| p == terminate_path()),
        "SIG{signal} must call the app's /terminate hook"
    );
    assert!(!stderr.contains("warning"), "{stderr}");
}

#[test]
fn sigterm_calls_the_terminate_hook() {
    assert_clean_shutdown_on("TERM");
}

#[test]
fn sigint_calls_the_terminate_hook() {
    assert_clean_shutdown_on("INT");
}

#[test]
fn failed_terminate_hook_is_reported() {
    let (url, _paths) = app(500);
    let child = start_dev(&url);
    kill(child.id(), "TERM");
    let (status, stderr) = finish(child);
    assert!(status.success(), "{status}: {stderr}");
    assert!(
        stderr.contains("warning: terminate hook returned 500"),
        "{stderr}"
    );
}
