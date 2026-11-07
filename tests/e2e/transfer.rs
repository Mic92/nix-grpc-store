use std::time::Instant;

use crate::fixtures::*;

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

/// `nix log --store grpc://…` reads the log the server's nix-daemon kept,
/// by derivation and by output. It asks that store, then the substituters
/// (here cache.nixos.org, which the VM cannot reach), never the local store.
#[test]
fn build_log_is_read_from_the_server() {
    write_file(
        "/tmp/logged.nix",
        &drv_expr("logged-grpc", "echo line-from-the-build >&2; echo logged > $out"),
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
    // Anonymous, on the port with access control: read-only is enough.
    wait_for_unit("nix-grpc-daemon-mtls.service");
    wait_for_open_port(50052);
    let log = succeed(&["nix", "log", "--store", &anon_store(), drv]);
    assert!(log.contains("line-from-the-build"), "anonymous: {log}");

    // The log files as nix reads them: bzip2 streams one after another are
    // one log, an empty one included, and bytes after the last are ignored
    // unless they start a stream that does not decompress; a .bz2 that does
    // not decompress or cannot be read is no log, an empty file an empty log;
    // plain is tried before .bz2, the flat path after the bucketed one.
    let logs = |drv: &str| {
        let base = drv.rsplit('/').next().unwrap().to_string();
        (format!("/nix/var/log/nix/drvs/{}/{}", &base[..2], &base[2..]), format!("/nix/var/log/nix/drvs/{base}"))
    };
    let log_of = |drv: &str| run_t(&["nix", "log", "--store", STORE, drv], 300);
    let lines = |log: &str| log.matches("line-from-the-build").count();
    let bz2 = format!("{}.bz2", logs(drv).0);
    succeed(&["cp", &bz2, "/tmp/one.bz2"]);
    // The logs of `drv` here and through the server once `script` rewrote
    // its .bz2 ($1) from /tmp/one.bz2, a single stream.
    let after = |script: &str| {
        succeed(&["sh", "-c", &format!("cd /tmp && {script}"), "sh", &bz2]);
        (log_of(drv), run_t(&["nix", "log", drv], 300))
    };
    let (grpc, local) = after(r#"{ cat one.bz2 one.bz2; head -c 300000 /dev/urandom; } > "$1""#);
    assert!(grpc.code == 0 && lines(&grpc.stdout) == 2 && grpc.stdout == local.stdout, "two streams: {}", grpc.combined());
    let (grpc, local) = after(r#"{ printf 'BZh9\027rE8P\220\000\000\000\000'; cat one.bz2 one.bz2; } > "$1""#);
    assert!(grpc.code == 0 && lines(&grpc.stdout) == 2 && grpc.stdout == local.stdout, "empty first stream: {}", grpc.combined());
    // The bounds of libarchive's check: 13 bytes cannot start a stream, nor
    // can bytes whose block magic is wrong in its last (10th) byte; 14 can.
    let (grpc, local) = after(r#"{ cat one.bz2; head -c 13 one.bz2; } > "$1""#);
    assert!(grpc.code == 0 && lines(&grpc.stdout) == 1 && grpc.stdout == local.stdout, "13 bytes after: {}", grpc.combined());
    let (grpc, local) = after(r#"{ cat one.bz2; head -c 14 one.bz2; } > "$1""#);
    assert!(grpc.stderr.contains("is not available") && local.code != 0, "14 bytes after: {}", grpc.combined());
    let (grpc, local) = after(r#"{ cat one.bz2; head -c 9 one.bz2; printf X; tail -c +11 one.bz2; } > "$1""#);
    assert!(grpc.code == 0 && lines(&grpc.stdout) == 1 && grpc.stdout == local.stdout, "10th byte off: {}", grpc.combined());
    let crc = r#"cat one.bz2 one.bz2 > "$1" && printf XXXX | dd of="$1" bs=1 seek=$(($(stat -c %s one.bz2) + 10)) conv=notrunc"#;
    let (grpc, local) = after(crc);
    assert!(grpc.stderr.contains("is not available") && local.code != 0, "second stream fails its CRC: {}", grpc.combined());
    let (grpc, local) = after(r#"cat one.bz2 one.bz2 | head -c $((2 * $(stat -c %s one.bz2) - 1)) > "$1""#);
    assert!(grpc.stderr.contains("is not available") && local.code != 0, "second stream cut short: {}", grpc.combined());
    succeed(&["cp", "/tmp/one.bz2", &bz2]);
    // A .bz2 that stops decompressing once a chunk was sent fails the call
    // (DATA_LOSS) with nothing appended, where nix serves the next file.
    succeed(&["nix", "build", "--store", STORE, "--impure", "-f", "/etc/hugelog.nix", "--no-link"]);
    let huge = succeed(&["nix-instantiate", "/etc/hugelog.nix"]);
    write_file("/tmp/bigcut.nix", &drv_expr("bigcut-grpc", "echo never > $out"));
    let bigcut = succeed(&["nix-instantiate", "/tmp/bigcut.nix"]);
    let ((huge_log, _), (cut, cut_flat)) = (logs(huge.trim()), logs(bigcut.trim()));
    let script = r#"mkdir -p "${2%/*}" && head -c 2000000 "$1.bz2" > "$2.bz2" && echo flat-line > "$3""#;
    succeed(&["sh", "-c", script, "sh", &huge_log, &cut, &cut_flat]);
    let (grpc, local) = (log_of(bigcut.trim()), run_t(&["nix", "log", bigcut.trim()], 300));
    let out = grpc.stdout.len();
    assert!(grpc.stderr.contains("does not decompress") && local.stdout.contains("flat-line"), "cut after a chunk ({out} bytes out): {}", grpc.stderr);
    write_file("/tmp/unbuilt.nix", &drv_expr("unbuilt-grpc", "echo never > $out"));
    let unbuilt = succeed(&["nix-instantiate", "/tmp/unbuilt.nix"]);
    let unbuilt = unbuilt.trim();
    let (bucketed, flat) = logs(unbuilt);
    let err = fail(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(err.contains("is not available"), "{err}");
    succeed(&["sh", "-c", r#"mkdir -p "${1%/*}" && head -c 1000 /dev/urandom > "$1.bz2""#, "sh", &bucketed]);
    let err = fail(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(err.contains("is not available"), "corrupt .bz2: {err}");
    write_file(&flat, "flat-line\n");
    let log = succeed(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(log.contains("flat-line"), "flat: {log}");
    succeed(&["sh", "-c", r#": > "$1.bz2""#, "sh", &bucketed]);
    let log = succeed(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(log.is_empty() && succeed(&["nix", "log", unbuilt]).is_empty(), "empty .bz2: {log}");
    write_file(&bucketed, "plain-line\n");
    let log = succeed(&["nix", "log", "--store", STORE, unbuilt]);
    assert!(log.contains("plain-line"), "plain before .bz2: {log}");
    // A plain file nix cannot read is an error, a dangling symlink or a loop
    // included: pathExists is lstat. Not a lost nix-daemon, to retry.
    for target in ["/nonexistent", bucketed.as_str()] {
        succeed(&["ln", "-sfn", target, &bucketed]);
        let (grpc, local) = (log_of(unbuilt), run_t(&["nix", "log", unbuilt], 300));
        let err = grpc.combined();
        assert!(err.contains("opening '") && !err.contains("connection lost") && local.code != 0, "symlink to {target}: {err}");
    }
    succeed(&["mv", &bucketed, "/tmp/dangling-log"]);
    // A .bz2 that cannot be opened or read is skipped.
    succeed(&["chmod", "000", &format!("{bucketed}.bz2")]);
    let grpc = log_of(unbuilt);
    assert!(grpc.stdout.contains("flat-line"), "unreadable .bz2: {}", grpc.combined());
    succeed(&["sh", "-c", r#"mv "$1.bz2" /tmp/unreadable.bz2 && mkdir "$1.bz2""#, "sh", &bucketed]);
    let grpc = log_of(unbuilt);
    assert!(grpc.stdout.contains("flat-line"), ".bz2 is a directory: {}", grpc.combined());
    // A bucket that is not a directory: ENOTDIR, which nix's pathExists skips.
    let (bucket, own) = bucketed.rsplit_once('/').unwrap();
    let entries = succeed(&["ls", "-A", bucket]);
    assert!(entries.lines().all(|e| e.starts_with(own)), "other logs in {bucket}: {entries}");
    succeed(&["sh", "-c", r#"mv "$1" /tmp/bucket && echo > "$1""#, "sh", bucket]);
    let grpc = log_of(unbuilt);
    succeed(&["sh", "-c", r#"mv "$1" /tmp/bucket-file && mv /tmp/bucket "$1""#, "sh", bucket]);
    assert!(grpc.stdout.contains("flat-line"), "bucket is a file: {}", grpc.combined());
    // Unbuilt has no log again, for the next run.
    succeed(&["sh", "-c", r#"mv "$1.bz2" "$2" "$(mktemp -d)""#, "sh", &bucketed, &flat]);
}

/// The `printf` format of a GetBuildLog request for `drv_path` as a gRPC
/// message: uncompressed, its length, then field 1.
pub(crate) fn get_build_log_request(drv_path: &str) -> String {
    assert!(drv_path.len() < 126 && !drv_path.contains(['%', '\\']));
    format!(
        "\\000\\000\\000\\000\\{:03o}\\012\\{:03o}{drv_path}",
        drv_path.len() + 2,
        drv_path.len()
    )
}

/// A shell function calling GetBuildLog with curl, as a raw gRPC call: its
/// first argument is the request's `printf` format, the rest are curl's.
pub(crate) const GET_BUILD_LOG: &str = r#"get_build_log() {
  printf "$1" > "/tmp/req.$$"; shift
  curl -sS --http2-prior-knowledge -H 'content-type: application/grpc' -H 'te: trailers' \
    --data-binary "@/tmp/req.$$" "$@" http://127.0.0.1:50051/nix.remote.NixRemote/GetBuildLog
}"#;

pub(crate) struct StopUnit(pub(crate) &'static str);

impl Drop for StopUnit {
    fn drop(&mut self) {
        run_t(&["systemctl", "stop", self.0], 60);
    }
}

/// A client that opens a build log and stops reading holds up no other
/// `nix log`, and the daemon has read only part of the log file meanwhile.
/// Once the reader is gone, the log arrives whole. It is far larger than the
/// socket buffers and HTTP/2 windows in between, so the daemon is still
/// sending it while the reader stalls.
#[test]
fn a_reader_that_stops_reading_holds_up_no_other_build_log() {
    let huge = succeed(&["nix-instantiate", "/etc/hugelog.nix"]);
    succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/etc/hugelog.nix",
        "--no-link",
    ]);
    write_file(
        "/tmp/small-log.nix",
        &drv_expr("small-log-grpc", "echo small-log-line >&2; echo small > $out"),
    );
    succeed(&[
        "nix",
        "build",
        "--store",
        STORE,
        "--impure",
        "-f",
        "/tmp/small-log.nix",
        "--no-link",
    ]);
    let small = succeed(&["nix-instantiate", "/tmp/small-log.nix"]);

    let unit = "stalled-build-log";
    let request = get_build_log_request(huge.trim().rsplit('/').next().unwrap());
    let stop = StopUnit(unit);
    // The first byte goes to /tmp/stalled.first; then nobody reads the pipe.
    succeed(&[
        "systemd-run",
        "--unit",
        unit,
        "--collect",
        "--setenv=PATH=/run/current-system/sw/bin",
        "sh",
        "-c",
        &format!(
            "{GET_BUILD_LOG}\nget_build_log \"$1\" | {{ dd bs=1 count=1 of=/tmp/stalled.first 2>/dev/null; sleep 600; }}"
        ),
        "sh",
        &request,
    ]);
    wait_until_succeeds(&["test", "-s", "/tmp/stalled.first"], 120);

    // A short retry budget: a call refused while the other holds on fails
    // in seconds instead of minutes.
    let store = format!("{STORE}&connect-timeout=5&restart-grace=5");
    let o = run_t(&["nix", "log", "--store", &store, small.trim()], 120);
    assert!(
        o.code == 0 && o.stdout.contains("small-log-line"),
        "nix log while another reader stalls: {}",
        o.combined()
    );
    // The daemon streams the log file: it is open, and only partly read.
    let read = open_log_files("hugelog-grpc");
    assert!(read.len() == 1 && read[0][0] < read[0][1], "the daemon's open log file (read, size): {read:?}");
    drop(stop);
    wait_until_succeeds(
        &[
            "sh",
            "-c",
            r#"nix log --store "$1" "$2" > /tmp/huge.grpc && nix log "$2" | cmp - /tmp/huge.grpc"#,
            "sh",
            &store,
            huge.trim(),
        ],
        60,
    );
}

/// The read offset and size of each log file of the derivation `name` that
/// the daemon has open.
pub(crate) fn open_log_files(name: &str) -> Vec<Vec<u64>> {
    let open = succeed(&[
        "sh",
        "-c",
        r#"pid=$(systemctl show -p MainPID --value nix-grpc-daemon)
for fd in /proc/$pid/fd/*; do
  case "$(readlink "$fd")" in
    *-"$1".drv*) echo "$(awk '/^pos:/ {print $2}' "/proc/$pid/fdinfo/${fd##*/}") $(stat -L -c %s "$fd")" ;;
  esac
done"#,
        "sh",
        name,
    ]);
    open.lines().map(|l| l.split_whitespace().map(|n| n.parse().unwrap()).collect()).collect()
}

/// A name that is not a store path is the caller's error.
#[test]
fn build_log_of_a_bad_name_is_invalid_argument() {
    let call = format!("{GET_BUILD_LOG}\nget_build_log \"$1\" -D - -o /dev/null");
    let headers = succeed(&["sh", "-c", &call, "sh", &get_build_log_request("not-a-store-path")]);
    assert!(
        headers.to_ascii_lowercase().contains("grpc-status: 3\r"),
        "{headers}"
    );
}

/// Only the `.bz2` candidate is decompressed: a log whose name ends in
/// `.bz2` is read as the plain file it is.
#[test]
fn build_log_named_bz2_is_plain() {
    let name = "00000000000000000000000000000000-named.bz2";
    let path = format!("/nix/var/log/nix/drvs/00/{}", &name[2..]);
    succeed(&["sh", "-c", r#"mkdir -p "${1%/*}" && echo plain > "$1""#, "sh", &path]);
    let call = format!("{GET_BUILD_LOG}\nget_build_log \"$1\" -D - -o /dev/null");
    let headers = succeed(&["sh", "-c", &call, "sh", &get_build_log_request(name)]);
    assert!(headers.to_ascii_lowercase().contains("grpc-status: 0\r"), "{headers}");
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
    for f in &files {
        random_file(f, 4096);
    }
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
