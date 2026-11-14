use super::helpers::*;
use crate::harness::*;

#[test]
fn outputs_carry_the_cache_signature() {
    farm_ready();
    let pubkey = env("NGS_SIGNING_PUBKEY");
    let signed = build(&envoy(), "sig", &[]);
    for w in WORKERS {
        let out = succeed(
            CLIENT,
            &["nix", "path-info", "--sigs", "--store", &direct(w), &signed],
        );
        let key = pubkey.split(':').next().unwrap();
        assert!(out.contains(&format!("{key}:")), "{} {out}", w.0);
    }
    succeed(CLIENT, &["nix-store", "--delete", &signed]);
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--from",
            &envoy(),
            "--option",
            "trusted-public-keys",
            &pubkey,
            &signed,
        ],
    );
}

/// Queries and uploads do not need a builder of the client's system.
#[test]
fn a_system_without_builders_is_served_by_the_default_one() {
    farm_ready();
    let store = format!("{}&system=s390x-linux", envoy());
    succeed(CLIENT, &["nix", "store", "info", "--store", &store]);
}

#[test]
fn build_hook_through_nix_daemon() {
    farm_ready();
    let hook = hook();
    let job = expr("JOB");
    let mut cmd = vec![
        "sh",
        "-c",
        r#"NIX_REMOTE=daemon exec "$@" 2>/tmp/hook.log"#,
        "sh",
        "nix",
        "build",
        "--log-format",
        "internal-json",
    ];
    cmd.extend(hook.iter().map(String::as_str));
    cmd.extend([
        "--print-out-paths",
        "--no-link",
        "-f",
        &job,
        "--argstr",
        "tag",
        "hook",
    ]);
    let out = succeed(CLIENT, &cmd);
    let built = succeed(CLIENT, &["cat", out.trim()]);
    assert!(built.contains("farm-top-hook"), "{built}");
    let log = succeed(CLIENT, &["cat", "/tmp/hook.log"]);
    let has = |parts: &[&str]| log.lines().any(|l| parts.iter().all(|p| l.contains(p)));
    assert!(has(&["LOG-farm-top-hook", r#""type":101"#]), "{log}");
    assert!(has(&["farmPhase", r#""type":104"#]), "{log}");
    assert!(
        has(&[r#""type":105"#, "lb:50051", r#""fields":["/nix/store/"#]),
        "{log}"
    );
}

#[test]
fn an_input_only_one_worker_has_still_reaches_the_builder() {
    farm_ready();
    let dep = expr("DEP");
    let hook = hook();
    let inp = succeed(
        CLIENT,
        &[
            "nix-build",
            "--no-out-link",
            &dep,
            "-A",
            "input",
            "--argstr",
            "tag",
            "w1only",
        ],
    );
    let inp = inp.trim();
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"nix-store --export "$1" > /tmp/shared/inp.closure"#,
            "sh",
            inp,
        ],
    );
    succeed(
        WORKER1,
        &["sh", "-c", "nix-store --import < /tmp/shared/inp.closure"],
    );
    fail(WORKER2, &["test", "-e", inp]);
    for salt in ["a", "b", "c"] {
        let mut cmd = vec![
            "sh",
            "-c",
            r#"NIX_REMOTE=daemon exec "$@" >&2"#,
            "sh",
            "nix",
            "build",
            "-L",
        ];
        cmd.extend(hook.iter().map(String::as_str));
        cmd.extend([
            "--no-link",
            "-f",
            &dep,
            "job",
            "--argstr",
            "tag",
            "w1only",
            "--argstr",
            "salt",
            salt,
        ]);
        succeed(CLIENT, &cmd);
    }
}

/// Requests per worker of `cluster`, as envoy counts them.
fn requests(cluster: &str) -> Vec<(Node, i64)> {
    let mut found: Vec<(Node, i64)> = succeed(LB, &["curl", "-sf", "localhost:9901/clusters"])
        .lines()
        .filter_map(|l| {
            let mut p = l.split("::");
            let (c, addr, key, n) = (p.next()?, p.next()?, p.next()?, p.next()?);
            (c == cluster && key == "rq_total").then_some(())?;
            let ip = addr.split(':').next()?;
            let w = WORKERS
                .into_iter()
                .find(|w| env(&format!("NGS_{}_IP", w.0.to_uppercase())) == ip)?;
            Some((w, n.parse().ok()?))
        })
        .collect();
    found.sort_by_key(|(n, _)| n.0);
    found
}

/// A system other than the default routes through an aggregate cluster that
/// falls back to the default system's builders. A request naming its worker
/// must still reach that worker: one that reaches another is refused there as
/// "not scheduled here", and the client gives up after a few rounds.
#[test]
fn a_named_worker_gets_its_requests_on_a_system_other_than_the_default() {
    farm_ready();
    let cluster = "riscv64-linux";
    wait_members(cluster, &WORKERS);
    let worker = format!("{}:50051", env("NGS_WORKER1_IP"));
    let c = certs();
    let before = requests(cluster);
    let tries = 20;
    let mut answers = Vec::new();
    for _ in 0..tries {
        // Any method will do: envoy counts the request at the host it picked.
        let o = run_t(
            LB,
            &[
                "curl",
                "-sS",
                "--http2",
                "-D",
                "-",
                "-o",
                "/dev/null",
                "--cacert",
                &format!("{c}/public-ca.pem"),
                "--cert",
                &format!("{c}/ci.pem"),
                "--key",
                &format!("{c}/ci.key"),
                "-H",
                "content-type: application/grpc",
                "-H",
                "te: trailers",
                "-H",
                &format!("x-nix-system: {cluster}"),
                "-H",
                &format!("x-nix-worker: {worker}"),
                "--data-binary",
                "",
                "https://lb:50051/nix.remote.NixRemote/Probe",
            ],
            30,
        );
        answers.push(o.combined());
    }
    let sent: Vec<(Node, i64)> = requests(cluster)
        .into_iter()
        .zip(before)
        .map(|((n, after), (_, b))| (n, after - b))
        .collect();
    assert_eq!(
        sent,
        vec![(WORKER1, tries), (WORKER2, 0)],
        "first answer: {}",
        answers[0]
    );
}
