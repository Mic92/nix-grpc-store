use std::sync::OnceLock;

use crate::harness::{self, MACHINE};

pub use crate::harness::env;

pub const STORE: &str = "grpc://127.0.0.1:50051?insecure=1";
pub const ENVOY_STORE: &str = "grpc://127.0.0.1:50060?insecure=1";

pub fn cert_dir() -> String {
    env("NGS_CERT_DIR")
}

pub fn cert_store(name: &str) -> String {
    let d = cert_dir();
    format!(
        "grpc://localhost:50052?ca-cert={d}/ca.pem&client-cert={d}/{name}.pem&client-key={d}/{name}.key"
    )
}

pub fn anon_store() -> String {
    format!("grpc://localhost:50052?ca-cert={}/ca.pem", cert_dir())
}

pub fn hello_path() -> &'static str {
    static P: OnceLock<String> = OnceLock::new();
    P.get_or_init(|| {
        succeed(&[
            "nix",
            "build",
            "--store",
            STORE,
            "--impure",
            "-f",
            "/etc/hello.nix",
            "--no-link",
            "--print-out-paths",
        ])
        .trim()
        .to_string()
    })
}

pub fn workdir(name: &str) -> String {
    let dir = format!("/root/{name}");
    succeed(&["mkdir", "-p", &dir]);
    dir
}

/// Writes `bytes` random bytes, base64-encoded, to `path`.
pub fn random_file(path: &str, bytes: usize) {
    succeed(&[
        "sh",
        "-c",
        r#"head -c "$1" /dev/urandom | base64 > "$2""#,
        "sh",
        &bytes.to_string(),
        path,
    ]);
}

pub fn deny_file(dir: &str) -> String {
    let path = format!("{dir}/denyfile");
    random_file(&path, 64);
    path
}

pub fn drv_expr(name: &str, script: &str) -> String {
    format!(
        r#"derivation {{ name = "{name}"; system = builtins.currentSystem; builder = "/bin/sh"; args = [ "-c" "{script}" ]; }}"#
    )
}

pub fn succeed(argv: &[&str]) -> String {
    harness::succeed(MACHINE, argv)
}

pub fn fail(argv: &[&str]) -> String {
    harness::fail(MACHINE, argv)
}

pub fn run_t(argv: &[&str], secs: u64) -> harness::Out {
    harness::run_t(MACHINE, argv, secs)
}

pub fn wait_until_succeeds(argv: &[&str], secs: u64) {
    harness::wait_until_succeeds(MACHINE, argv, secs);
}

pub fn wait_for_unit(unit: &str) {
    harness::wait_for_unit(MACHINE, unit);
}

pub fn wait_for_open_port(port: u16) {
    harness::wait_for_open_port(MACHINE, port);
}

pub fn unit_state(unit: &str) -> String {
    harness::unit_state(MACHINE, unit)
}

pub fn assert_journal(unit: &str, pattern: &str) {
    harness::assert_journal(MACHINE, unit, pattern);
}

pub fn assert_no_journal(unit: &str, pattern: &str) {
    harness::assert_no_journal(MACHINE, unit, pattern);
}

pub fn wait_journal_above(unit: &str, pattern: &str, before: u64, secs: u64) {
    harness::wait_journal_above(MACHINE, unit, pattern, before, secs);
}

pub fn write_file(path: &str, content: &str) {
    harness::write_file(MACHINE, path, content);
}
