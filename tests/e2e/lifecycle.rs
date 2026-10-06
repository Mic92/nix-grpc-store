use std::time::{Duration, Instant};

use crate::fixtures::*;
use crate::harness::{retry, sleep};

#[test]
fn idle_exit_and_socket_activation() {
    succeed(&["systemctl", "stop", "nix-grpc-daemon.service"]);
    assert_eq!(unit_state("nix-grpc-daemon.service"), "inactive");
    let out = succeed(&["nix", "store", "info", "--json", "--store", STORE]);
    assert!(out.contains(r#""url":"grpc://127.0.0.1:50051"#), "{out}");
    assert_eq!(unit_state("nix-grpc-daemon.service"), "active");
    retry(30, "the daemon exits when idle", || {
        unit_state("nix-grpc-daemon.service") == "inactive"
    });
    assert_journal("nix-grpc-daemon", "event=idle_exit");
    succeed(&["nix", "store", "info", "--store", STORE]);
}

#[test]
fn nix_daemon_restart_does_not_fail_the_next_rpc() {
    let p = hello_path();
    write_file("/tmp/warm", "warm");
    succeed(&["nix", "store", "add", "--store", STORE, "/tmp/warm"]);
    assert_journal(
        "nix-daemon",
        "accepted connection from pid .*nix-grpc-daemon",
    );
    // SIGKILL, like a crashed container: no orderly close of the pooled connections.
    succeed(&["systemctl", "kill", "-s", "KILL", "nix-daemon.service"]);
    wait_until_succeeds(
        &["sh", "-c", "! systemctl is-active nix-daemon.service"],
        30,
    );
    succeed(&["systemctl", "start", "nix-daemon.socket"]);
    succeed(&["nix", "path-info", "--store", STORE, p]);
    write_file("/tmp/warm", "again");
    succeed(&["nix", "store", "add", "--store", STORE, "/tmp/warm"]);
}

#[test]
fn renewed_server_certificate_is_served_without_restart() {
    wait_for_unit("nix-grpc-daemon-mtls.service");
    wait_for_open_port(50052);
    let d = cert_dir();
    succeed(&[
        "bash",
        "-c",
        r#"cd "$1" &&
           openssl req -newkey rsa:2048 -nodes -keyout new.key -out new.csr -subj /CN=localhost 2>/dev/null &&
           openssl x509 -req -in new.csr -days 1 -CA ca.pem -CAkey ca.key -set_serial 0x$RANDOM \
             -extfile <(printf 'subjectAltName=DNS:localhost,DNS:renewed.example') -out new.pem &&
           mv new.key server.key && mv new.pem server.pem"#,
        "bash",
        &d,
    ]);
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        // s_client may exit non-zero because the server wants a client
        // certificate, so only the printed SANs count.
        let sans = succeed(&[
            "sh",
            "-c",
            "openssl s_client -connect localhost:50052 </dev/null 2>/dev/null \
             | openssl x509 -noout -ext subjectAltName || true",
        ]);
        if sans.contains("renewed.example") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the server still presents the old certificate, SANs: {sans}"
        );
        sleep(1);
    }
    succeed(&[
        "nix",
        "store",
        "info",
        "--json",
        "--store",
        &cert_store("client"),
    ]);
}

#[test]
fn default_client_cert_lookup() {
    wait_for_unit("nix-grpc-daemon-mtls.service");
    wait_for_open_port(50052);
    let d = cert_dir();
    succeed(&["install", "-d", "/var/lib/nix-grpc-store"]);
    succeed(&[
        "install",
        "-m",
        "0644",
        &format!("{d}/client.pem"),
        "/var/lib/nix-grpc-store/client.crt",
    ]);
    succeed(&[
        "install",
        "-m",
        "0600",
        &format!("{d}/client.key"),
        "/var/lib/nix-grpc-store/client.key",
    ]);
    let out = run_t(
        &[
            "nix",
            "store",
            "add",
            "--store",
            &anon_store(),
            "/etc/hello.nix",
        ],
        120,
    );
    succeed(&["rm", "-r", "/var/lib/nix-grpc-store"]);
    assert_eq!(out.code, 0, "{}", out.combined());
}
