use std::time::Instant;

use crate::fixtures::*;
use crate::harness::{MACHINE, metrics, retry};

#[test]
fn add_path_and_read_it_back_locally() {
    let p = succeed(&["nix", "store", "add", "--store", STORE, "/etc/hello.nix"]);
    let p = p.trim();
    succeed(&["nix", "path-info", p]);
    succeed(&["test", "-e", p]);
}

#[test]
fn build_over_grpc() {
    let p = hello_path();
    let built = succeed(&["cat", p]);
    assert!(built.contains("hello-over-grpc"), "{built}");
}

#[test]
fn build_whose_log_is_not_utf8() {
    let p = succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/etc/rawlog.nix",
        "--no-link",
        "--print-out-paths",
    ]);
    let built = succeed(&["cat", p.trim()]);
    assert!(built.contains("rawlog-over-grpc"), "{built}");
}

/// `nix log --store grpc://…` reads the server's log, by derivation and by output.
#[test]
fn build_log_is_read_from_the_server() {
    write_file(
        "/tmp/logged.nix",
        &drv_expr(
            "logged-grpc",
            "echo line-from-the-build >&2; echo logged > $out",
        ),
    );
    let out = succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/tmp/logged.nix",
        "--no-link",
        "--print-out-paths",
    ]);
    let drv = succeed(&["nix-instantiate", "/tmp/logged.nix"]);
    let drv = drv.trim();
    for installable in [drv, out.trim()] {
        let log = succeed(&["nix", "log", "--store", STORE, installable]);
        assert!(log.contains("line-from-the-build"), "{installable}: {log}");
    }
    // read-only is enough, so anonymous callers get it too.
    wait_for_unit("nix-grpc-daemon-mtls.service");
    wait_for_open_port(50052);
    let log = succeed(&["nix", "log", "--store", &anon_store(), drv]);
    assert!(log.contains("line-from-the-build"), "anonymous: {log}");

    // Missing, dangling, truncated .bz2, and .bz2 with trailing garbage.
    write_file(
        "/tmp/unbuilt.nix",
        &drv_expr("unbuilt-grpc", "echo never > $out"),
    );
    let unbuilt = succeed(&["nix-instantiate", "/tmp/unbuilt.nix"]);
    let unbuilt = unbuilt.trim();
    let logs = |drv: &str| {
        let base = drv.rsplit('/').next().unwrap().to_string();
        (
            format!("/nix/var/log/nix/drvs/{}/{}", &base[..2], &base[2..]),
            base,
        )
    };
    let (bucketed, base) = logs(unbuilt);
    let err = fail(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(err.contains("is not available"), "no log: {err}");
    succeed(&[
        "sh",
        "-c",
        r#"mkdir -p "${1%/*}" && ln -sfn /nonexistent "$1""#,
        "sh",
        &bucketed,
    ]);
    let err = fail(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(
        err.contains("cannot open") && !err.contains("connection lost"),
        "dangling: {err}"
    );
    let script = r#"mv "$1" /tmp/dangling-log
head -c 2000000 /dev/urandom | bzip2 | head -c 1500000 > "$1.bz2""#;
    succeed(&["sh", "-c", script, "sh", &bucketed]);
    let err = fail(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(err.contains("does not decompress"), "cut short: {err}");
    let script = r#"{ cat "$2.bz2" "$2.bz2"; head -c 1000 /dev/urandom; } > "$1.bz2""#;
    succeed(&["sh", "-c", script, "sh", &bucketed, &logs(drv).0]);
    let log = succeed(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(
        log.matches("line-from-the-build").count() == 2,
        "two streams: {log}"
    );
    // A 256 MiB log must be streamed, not held in memory.
    let script = format!(
        r#"{GET_BUILD_LOG}
head -c 256M /dev/zero | bzip2 > "$2.bz2"
nix log --store "$3" "$4" > /dev/null
pid=$(systemctl show -p MainPID --value nix-grpc-daemon)
echo 5 > /proc/$pid/clear_refs
get_build_log "$1" -D /tmp/bomb.hdr -o /dev/null
grep -qi 'grpc-status: 0' /tmp/bomb.hdr
test "$(systemctl show -p MainPID --value nix-grpc-daemon)" = "$pid"
awk '/^VmHWM/ {{print $2}}' /proc/$pid/status"#
    );
    let peak = succeed(&[
        "sh",
        "-c",
        &script,
        "sh",
        &get_build_log_request(&base),
        &bucketed,
        STORE,
        drv,
    ]);
    let peak: u64 = peak.trim().parse().unwrap();
    assert!(
        peak < 160 * 1024,
        "daemon peak RSS {peak} kB for a 256 MiB log"
    );
}

/// GetBuildLog request as a `printf` format of the raw gRPC frame.
pub(crate) fn get_build_log_request(drv_path: &str) -> String {
    assert!(drv_path.len() < 126 && !drv_path.contains(['%', '\\']));
    format!(
        "\\000\\000\\000\\000\\{:03o}\\012\\{:03o}{drv_path}",
        drv_path.len() + 2,
        drv_path.len()
    )
}

/// Shell function calling GetBuildLog via curl. Args: request format, then curl flags.
pub(crate) const GET_BUILD_LOG: &str = r#"get_build_log() {
  printf "$1" > "/tmp/req.$$"; shift
  curl -sS --http2-prior-knowledge -H 'content-type: application/grpc' -H 'te: trailers' \
    --data-binary "@/tmp/req.$$" "$@" http://127.0.0.1:50051/nix.remote.NixRemote/GetBuildLog
}"#;

#[test]
fn build_log_of_a_bad_name_is_invalid_argument() {
    let call = format!("{GET_BUILD_LOG}\nget_build_log \"$1\" -D - -o /dev/null");
    let headers = succeed(&[
        "sh",
        "-c",
        &call,
        "sh",
        &get_build_log_request("not-a-store-path"),
    ]);
    assert!(
        headers.to_ascii_lowercase().contains("grpc-status: 3\r"),
        "{headers}"
    );
}

#[test]
fn copy_to_a_local_scratch_store() {
    let p = hello_path();
    let dir = workdir("scratch");
    succeed(&[
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        STORE,
        "--to",
        &format!("{dir}/store"),
        p,
    ]);
    let name = p.rsplit('/').next().unwrap();
    succeed(&["test", "-e", &format!("{dir}/store/nix/store/{name}")]);
}

#[test]
fn bulk_upload_is_not_pinned_after_the_rpc_returns() {
    let dir = workdir("bulk");
    succeed(&[
        "dd",
        "if=/dev/urandom",
        &format!("of={dir}/blob"),
        "bs=1M",
        "count=128",
        "status=none",
    ]);
    let up = succeed(&[
        "nix",
        "store",
        "add",
        "--store",
        &format!("{dir}/src"),
        "--mode",
        "flat",
        &format!("{dir}/blob"),
    ]);
    let up = up.trim();
    succeed(&[
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        &format!("{dir}/src"),
        "--to",
        STORE,
        up,
    ]);
    succeed(&["test", "-e", up]);
    succeed(&["nix-store", "--delete", up]);
    fail(&["test", "-e", up]);
}

#[test]
fn many_small_paths_round_trip() {
    let dir = workdir("small");
    succeed(&["mkdir", "-p", &format!("{dir}/files")]);
    let files: Vec<String> = (0..200).map(|i| format!("{dir}/files/f{i}")).collect();
    random_files(&files, 4096);
    let mut add = vec!["nix-store", "--store", "", "--add"];
    let src = format!("{dir}/src");
    add[2] = &src;
    add.extend(files.iter().map(String::as_str));
    let added = succeed(&add);
    let small: Vec<&str> = added.lines().collect();
    assert_eq!(small.len(), 200, "{added}");

    let t0 = Instant::now();
    let mut copy_up = vec![
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        &src,
        "--to",
        STORE,
    ];
    copy_up.extend(&small);
    succeed(&copy_up);
    println!(
        "[bench] small/upload   {:6.2}s (200 paths)",
        t0.elapsed().as_secs_f64()
    );
    for p in &small[..3] {
        succeed(&["test", "-e", p]);
    }

    let t0 = Instant::now();
    let dst = format!("{dir}/dst");
    let mut copy_down = vec![
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        STORE,
        "--to",
        &dst,
    ];
    copy_down.extend(&small);
    succeed(&copy_down);
    println!(
        "[bench] small/download {:6.2}s (200 paths)",
        t0.elapsed().as_secs_f64()
    );
    for p in &small[..3] {
        let name = p.rsplit('/').next().unwrap();
        succeed(&[
            "cmp",
            &format!("{dir}/src/nix/store/{name}"),
            &format!("{dir}/dst/nix/store/{name}"),
        ]);
    }
}

#[test]
fn add_path_through_envoy() {
    wait_for_unit("envoy.service");
    wait_for_open_port(50060);
    let dir = workdir("envoy");
    random_file(&format!("{dir}/f"), 4096);
    let p = succeed(&[
        "nix",
        "store",
        "add",
        "--store",
        ENVOY_STORE,
        &format!("{dir}/f"),
    ]);
    succeed(&["test", "-e", p.trim()]);
    assert_journal("nix-grpc-daemon", "event=rpc method=Connect");
}

/// Writes /tmp/{name}.nix, a derivation that runs `script` with coreutils,
/// and returns its .drv path.
fn log_drv(name: &str, script: &str) -> String {
    let file = format!("/tmp/{name}.nix");
    write_file(
        &file,
        &format!(r#"import /etc/log-drv.nix {{ name = "{name}"; script = "{script}"; }}"#),
    );
    succeed(&["nix-instantiate", &file]).trim().to_string()
}

fn build_ok(store: &str, name: &str) -> bool {
    let file = format!("/tmp/{name}.nix");
    run_t(
        &[
            "nix",
            "build",
            "--store",
            store,
            "--impure",
            "-f",
            &file,
            "--no-link",
        ],
        300,
    )
    .code
        == 0
}

/// The daemon on 50051 copies build logs to a log store. A failed build's
/// log gets there, and `nix log --store` reads it.
#[test]
fn failed_build_log_reaches_the_log_store() {
    let drv = log_drv(
        "stored-grpc",
        "echo stored line one; echo stored line two; exit 1",
    );
    assert!(!build_ok(STORE, "stored-grpc"));
    let base = drv.rsplit('/').next().unwrap();
    wait_until_succeeds(&["test", "-s", &format!("/srv/logstore/log/{base}")], 60);
    let log = succeed(&["nix", "log", "--store", "http://127.0.0.1:8090", &drv]);
    assert!(log.contains("stored line two"), "{log}");
}

/// The daemon on 50054 uploads to a log store that takes 20 s per upload.
/// A log over 64 MiB is skipped and counted. A SIGTERM right after a
/// build's Done waits for its upload, which outlasts the 6 s the daemon
/// gives itself to shut down.
#[test]
fn log_store_skips_large_logs_and_drains() {
    const LOGSTORE: &str = "nix-grpc-daemon-logstore.service";
    let store = "grpc://127.0.0.1:50054?insecure=1";
    wait_for_unit(LOGSTORE);
    wait_for_open_port(50054);

    let big = log_drv(
        "huge-grpc",
        "head -c 70000000 /dev/zero | tr '\\\\0' x | fold -w 1000; echo > $out",
    );
    assert!(build_ok(store, "huge-grpc"));
    retry(60, "the huge log is skipped", || {
        metrics(MACHINE, 9465).iter().any(|line| {
            line.strip_prefix(r#"nix_grpc_build_log_uploads_total{result="skipped"} "#)
                .and_then(|value| value.parse::<f64>().ok())
                == Some(1.0)
        })
    });
    let big = big.rsplit('/').next().unwrap();
    fail(&["test", "-e", &format!("/srv/logstore/slow/log/{big}")]);

    let last = log_drv("last-grpc", "echo the last build; echo > $out");
    assert!(build_ok(store, "last-grpc"));
    succeed(&["systemctl", "kill", "-s", "TERM", LOGSTORE]);
    retry(90, "the daemon exits", || unit_state(LOGSTORE) != "active");
    let last = last.rsplit('/').next().unwrap();
    succeed(&["test", "-s", &format!("/srv/logstore/slow/log/{last}")]);
}
