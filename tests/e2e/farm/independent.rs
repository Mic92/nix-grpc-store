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

/// Storing the path would block on a client that runs on the same machine.
#[test]
fn a_worker_serves_a_path_it_lacks_without_storing_it() {
    farm_ready();
    let out = build(&envoy(), "served", &[]);
    for w in WORKERS {
        succeed(CLIENT, &["nix", "path-info", "--store", &direct(w), &out]);
        let dump = format!("nix store dump-path --store '{}' {out} >/dev/null", direct(w));
        succeed(CLIENT, &["sh", "-c", &dump]);
    }
    assert_eq!(holders(&out).len(), 1, "{:?}", holders(&out));
}

/// Queries and uploads do not need a builder of the client's system.
#[test]
fn a_system_without_builders_is_served_by_the_default_one() {
    farm_ready();
    let store = format!("{}&system=s390x-linux", envoy());
    succeed(CLIENT, &["nix", "store", "info", "--store", &store]);
}

/// The balancer forwards a client's SPIFFE ID, so a node's rule matches it.
#[test]
fn a_spiffe_rule_matches_a_client_behind_the_balancer() {
    farm_ready();
    let certs = certs();
    let store =
        format!("grpc://lb:50051?client-cert={certs}/svid.pem&client-key={certs}/svid.key");
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

/// Nix asks what is missing before it uploads the derivation.
#[test]
fn a_derivation_the_farm_has_not_seen_is_announced_as_a_build() {
    farm_ready();
    let argv = [
        "nix", "build", "--dry-run", "--store", &envoy(), "--eval-store", "auto", "-f", &expr("JOB"), "--argstr",
        "tag", "announced",
    ];
    let out = run_t(CLIENT, &argv, 60).combined();
    assert!(out.contains("will be built") && !out.contains("don't know how to build"), "{out}");
}

/// `nix log` against the cache shows why a build failed.
#[test]
fn the_log_of_a_failed_build_reaches_the_cache() {
    let build = ["--store", &envoy(), "--eval-store", "auto", "-f", &expr("FAIL"), "--argstr", "tag", "logged"];
    let mut argv = vec!["nix", "build", "--no-link"];
    argv.extend(build);
    let out = run_t(CLIENT, &argv, 120);
    assert!(out.code != 0, "{}", out.combined());
    let drv = succeed(CLIENT, &["nix", "eval", "--raw", "-f", &expr("FAIL"), "--argstr", "tag", "logged", "drvPath"]);
    let cache = env("NGS_NIKS3_URL");
    retry(60, "the log is in the cache", || {
        run_t(CLIENT, &["nix", "log", "--store", &cache, &drv], 30).combined().contains("BOOM-logged")
    });
}

#[test]
fn an_output_a_substituter_offers_unsigned_is_built() {
    farm_ready();
    let dep = expr("DEP");
    let offer = env("NGS_OFFER_URL");
    let tag = "offered1";
    // Built on the client only, then offered unsigned and deleted there.
    let out = succeed(
        CLIENT,
        &[
            "nix-build",
            "--no-out-link",
            &dep,
            "-A",
            "input",
            "--argstr",
            "tag",
            tag,
        ],
    );
    let out = out.trim();
    succeed(CLIENT, &["nix", "copy", "--to", &offer, out]);
    succeed(CLIENT, &["nix-store", "--delete", out]);
    for w in WORKERS {
        fail(w, &["test", "-e", out]);
        let dry = succeed(
            w,
            &[
                "sh",
                "-c",
                r#"NIX_REMOTE=daemon nix-store --realise --dry-run "$1" 2>&1"#,
                "sh",
                out,
            ],
        );
        assert!(
            dry.contains("will be fetched"),
            "{} is not offered {out}: {dry}",
            w.0
        );
    }
    let said = run_t(
        CLIENT,
        &["nix-store", "--store", &envoy(), "--check-validity", out],
        120,
    );
    eprintln!(
        "before the build, the farm answered {out} (exit {}): {}",
        said.code,
        said.combined()
    );
    let wants = || journal_count(leader(), UNIT, "event=want ");
    let before = wants();
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"exec "$@" >&2"#,
            "sh",
            "nix",
            "build",
            "-L",
            "--no-link",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &dep,
            "input",
            "--argstr",
            "tag",
            tag,
        ],
    );
    let wanted = wants().saturating_sub(before);
    assert!(wanted >= 1, "no build was sent for {out}");
    // The builders cannot fetch an unsigned path, so the build must have made it.
    let got = run_t(CLIENT, &["nix", "path-info", "--store", &envoy(), out], 120);
    assert_eq!(
        got.code,
        0,
        "the build succeeded, but the farm cannot give {out} ({wanted} wants sent): {}",
        got.combined()
    );
    assert!(
        !holders(out).is_empty(),
        "no builder holds {out} ({wanted} wants sent)"
    );
    // Checked last, so that a failure above shows whether a build was sent.
    assert_ne!(
        said.code,
        0,
        "before the build, the farm said it holds {out}: {}",
        said.combined()
    );
}
