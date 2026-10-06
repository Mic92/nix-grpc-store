use std::time::Instant;

use crate::fixtures::*;

fn copy_cmd<'a>(uri: &'a str, path: &'a str) -> Vec<&'a str> {
    vec![
        "nix",
        "copy",
        "--no-check-sigs",
        "--from",
        uri,
        "--to",
        "/root/bench",
        path,
    ]
}

fn blob(tag: &str) -> String {
    succeed(&[
        "nix",
        "build",
        "--impure",
        "-f",
        "/etc/blob.nix",
        tag,
        "--no-link",
        "--print-out-paths",
    ])
    .trim()
    .to_string()
}

fn fresh_bench_dir() {
    succeed(&["rm", "-rf", "/root/bench"]);
    succeed(&["mkdir", "-p", "/root/bench"]);
}

fn bench(label: &str, uri: &str, path: &str) -> f64 {
    fresh_bench_dir();
    let t0 = Instant::now();
    succeed(&copy_cmd(uri, path));
    let dt = t0.elapsed().as_secs_f64();
    println!("[bench] {label:16} {dt:6.2}s  {:6.1} MiB/s", 256.0 / dt);
    dt
}

#[test]
fn throughput_grpc_vs_unix_socket_daemon() {
    for tag in ["text", "rand"] {
        let path = blob(tag);
        bench(&format!("{tag}/warmup"), "daemon", &path);
        let unix = bench(&format!("{tag}/unix"), "daemon", &path);
        let grpc = bench(&format!("{tag}/grpc"), STORE, &path);
        println!("[bench] {tag}: grpc={:.2}x unix", grpc / unix);
    }
}

#[test]
fn perf_profile_of_grpc_copy() {
    let path = blob("rand");
    for (label, uri) in [("unix", "daemon"), ("grpc", STORE)] {
        fresh_bench_dir();
        // perf stat reports on stderr.
        let mut stat = vec![
            "perf",
            "stat",
            "-a",
            "-e",
            "task-clock,context-switches,cycles,instructions,cache-misses,syscalls:sys_enter_read,syscalls:sys_enter_write",
            "--",
        ];
        stat.extend(copy_cmd(uri, &path));
        let o = run_t(&stat, 600);
        assert_eq!(o.code, 0, "{}", o.combined());
        println!("[perf-stat {label}]\n{}", o.combined());
    }
    fresh_bench_dir();
    let mut record = vec![
        "perf",
        "record",
        "-a",
        "-g",
        "-F",
        "999",
        "-o",
        "/root/perf.data",
        "--",
    ];
    record.extend(copy_cmd(STORE, &path));
    succeed(&record);
    // Keep the report on the machine: with -g it is far too large to stream
    // back over ssh.
    let report = |sort: &[&str], lines: usize| {
        let mut argv = vec!["perf", "report", "-i", "/root/perf.data", "--stdio"];
        argv.extend(sort);
        argv.extend(["--percent-limit", "0.5"]);
        let script = format!("\"$@\" > /root/perf.txt 2>/dev/null && head -{lines} /root/perf.txt");
        let mut sh = vec!["sh", "-c", &script, "report"];
        sh.extend(argv);
        succeed(&sh)
    };
    println!(
        "[perf-report grpc top functions]\n{}",
        report(&["--no-children"], 80)
    );
    println!(
        "[perf-report grpc per-DSO]\n{}",
        report(&["--sort=dso"], 40)
    );
}
