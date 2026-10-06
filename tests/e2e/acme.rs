//! README access-control example: step-ca and the daemon on `server`, a
//! read-only ACME client on `host1`.
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use crate::harness::*;

const SERVER: Node = Node("server");
const HOST1: Node = Node("host1");

fn store_uri() -> String {
    "grpc://server:50051?ca-cert=/run/root_ca.crt\
     &client-cert=/var/lib/acme/host1/cert.pem&client-key=/var/lib/acme/host1/key.pem"
        .into()
}

fn cert_field(field: &str) -> String {
    succeed(
        HOST1,
        &[
            "openssl",
            "x509",
            "-in",
            "/var/lib/acme/host1/cert.pem",
            "-noout",
            &format!("-{field}"),
        ],
    )
}

// A placeholder self-signed cert sits there until the ACME order completes.
fn wait_for_acme_cert() {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let issuer = cert_field("issuer");
        if issuer.contains("Test Intermediate CA") {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "host1 still has a placeholder certificate, issuer: {issuer}"
        );
        sleep(1);
    }
}

fn signed_path() -> &'static str {
    static P: OnceLock<String> = OnceLock::new();
    P.get_or_init(|| {
        let p = succeed(
            SERVER,
            &[
                "nix",
                "build",
                "--impure",
                "-f",
                "/etc/hello.nix",
                "--no-link",
                "--print-out-paths",
            ],
        )
        .trim()
        .to_string();
        succeed(
            SERVER,
            &["nix", "store", "sign", "-k", "/etc/cache-key", &p],
        );
        p
    })
}

#[test]
fn host1_obtains_a_certificate_via_acme() {
    wait_for_acme_cert();
    let subject = cert_field("subject");
    assert!(
        subject.contains("CN") && subject.contains("host1"),
        "{subject}"
    );
}

#[test]
fn server_builds_and_signs_a_path() {
    let p = signed_path();
    let info = succeed(SERVER, &["nix", "path-info", "--json", p]);
    assert!(info.contains("nix-grpc-test-1:"), "not signed: {info}");
}

#[test]
fn host1_substitutes_the_signed_path_with_a_read_only_cert() {
    wait_for_acme_cert();
    let p = signed_path();
    fail(HOST1, &["test", "-e", p]);
    succeed(HOST1, &["nix-store", "-r", p]);
    let content = succeed(HOST1, &["cat", p]);
    assert!(content.contains("hello-over-grpc"), "{content}");
}

#[test]
fn a_read_only_host_cannot_write() {
    wait_for_acme_cert();
    write_file(HOST1, "/root/denyfile", "deny");
    fail(
        HOST1,
        &[
            "nix",
            "store",
            "add",
            "--store",
            &store_uri(),
            "/root/denyfile",
        ],
    );
    assert_journal(
        SERVER,
        "nix-grpc-daemon.service",
        "event=denied .*cn=host1 role=read-only",
    );
}
