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
