use std::time::Instant;

use super::helpers::*;
use crate::harness::*;

const UNIT: &str = "nix-grpc-daemon";

fn probe(query: &str) -> String {
    let system = succeed(CLIENT, &["readlink", "-f", "/run/current-system"]);
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "path-info",
            "--store",
            &format!("grpc://lb:50051?{query}"),
            system.trim(),
        ],
        120,
    );
    let out = o.combined();
    if o.code == 0 || out.contains("is not valid") {
        "ok".into()
    } else {
        out
    }
}

#[test]
fn balancer_auth_client_cert_token_nothing_unknown_cn() {
    farm_ready();
    let certs = certs();
    let aud = env("NGS_OIDC_AUDIENCE");
    assert_eq!(probe(&ci()), "ok");
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"curl -sfG "$@" > /root/dev.jwt && test -s /root/dev.jwt"#,
            "sh",
            "http://lb:8081/issue",
            "--data-urlencode",
            &format!("aud={aud}"),
            "--data-urlencode",
            "sub=dev:alice",
        ],
    );
    for (from, to) in [("foreign.pem", "client.crt"), ("foreign.key", "client.key")] {
        succeed(
            CLIENT,
            &[
                "install",
                "-D",
                &format!("{certs}/{from}"),
                &format!("/var/lib/nix-grpc-store/{to}"),
            ],
        );
    }
    let out = probe("token-file=/root/dev.jwt");
    succeed(CLIENT, &["rm", "-r", "/var/lib/nix-grpc-store"]);
    assert_eq!(out, "ok");
    let out = probe("");
    assert!(out.contains("client certificate or bearer token"), "{out}");
    let out = probe(&format!(
        "client-cert={certs}/stranger.pem&client-key={certs}/stranger.key"
    ));
    assert!(out.contains("no access rule matches 'stranger'"), "{out}");
}

#[test]
fn one_node_schedules_and_both_builders_hold_a_worker_session() {
    farm_ready();
    let ldr = leader();
    let opened = journal_matches(ldr, "nix-grpc-daemon", "event=worker_session_open").join("\n");
    assert!(
        opened.contains("cn=worker-1 ") && opened.contains("cn=oidc:mock:node:worker2 "),
        "worker sessions opened on the leader:\n{opened}"
    );
    assert_eq!(gauge(ldr, r#"nix_grpc_sched{kind="leader"}"#), 1);
    assert_eq!(gauge(standby(), r#"nix_grpc_sched{kind="leader"}"#), 0);
    let slots = gauge_sum(ldr, r#"kind="slots""#);
    assert!(slots >= 2, "{slots}");
    assert_eq!(gauge_sum(ldr, r#"kind="free""#), slots);
    assert_eq!(gauge_sum(ldr, r#"kind="queued""#), 0);
}

fn input_counts() -> (i64, i64) {
    WORKERS.into_iter().fold((0, 0), |(inputs, builds), w| {
        (
            inputs
                + gauge(w, r#"nix_grpc_build_inputs_total{state="local"}"#)
                + gauge(w, r#"nix_grpc_build_inputs_total{state="fetched"}"#),
            builds + gauge(w, "nix_grpc_build_input_seconds_count"),
        )
    })
}

#[test]
fn dag_build_is_scheduled_across_workers_and_lands_in_the_cache() {
    farm_ready();
    let (inputs0, builds0) = input_counts();
    let top = build(&envoy(), "t1", &[]);
    assert_eq!(holders(&top).len(), 1, "{:?}", holders(&top));
    fail(CLIENT, &["test", "-e", &top]);
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--from",
            &env("NGS_NIKS3_URL"),
            "--no-check-sigs",
            &top,
        ],
    );
    let content = succeed(CLIENT, &["cat", &top]);
    assert!(content.contains("farm-top-t1"), "{top} holds: {content}");
    let (inputs1, builds1) = input_counts();
    assert!(inputs1 - inputs0 >= 2, "{inputs0} -> {inputs1}");
    assert_eq!(builds1 - builds0, 3);
}

#[test]
fn repeat_is_answered_cached_by_the_scheduler_without_building() {
    farm_ready();
    let top = build(&envoy(), "cached1", &[]);
    for w in WORKERS {
        succeed(w, &["nix-store", "--delete", &top]);
    }
    let before = events(leader(), "cached");
    build(&envoy(), "cached1", &[]);
    assert!(holders(&top).is_empty(), "{:?}", holders(&top));
    assert!(events(leader(), "cached") > before);
}

#[test]
fn inputs_already_in_the_cache_are_not_sent_to_the_scheduler() {
    farm_ready();
    build(&envoy(), "wants1", &[]);
    let wants = || journal_count(leader(), UNIT, "event=want ");
    let before = wants();
    build(&envoy(), "wants1", &["--argstr", "top", "wants1b"]);
    assert_eq!(wants() - before, 1);
}

#[test]
fn two_clients_wanting_the_same_drv_build_it_once() {
    farm_ready();
    let attached = || {
        WORKERS
            .into_iter()
            .map(|w| events(w, "attached"))
            .sum::<i64>()
    };
    let before = attached();
    spawn_build("dedup1", "dedup");
    sleep(1);
    spawn_build("dedup2", "dedup");
    wait_building();
    sleep(2);
    release_slow();
    wait_unit_done("dedup1 dedup2", 60);
    for unit in ["dedup1", "dedup2"] {
        assert_unit_success(unit);
    }
    let mut ids: Vec<Vec<String>> = WORKERS
        .into_iter()
        .map(|w| {
            query(w, &["journalctl", "-u", "nix-grpc-daemon", "-o", "cat"])
                .lines()
                .filter(|l| l.contains("method=BuildDerivation") && l.contains("slow-dedup"))
                .filter_map(|l| l.split_whitespace().find(|t| t.starts_with("assign_id=")))
                .map(String::from)
                .collect()
        })
        .collect();
    ids.sort_by_key(Vec::len);
    let (idle, busy) = (&ids[0], &ids[1]);
    assert!(
        idle.is_empty() && busy.len() == 2 && busy[0] == busy[1],
        "{ids:?}"
    );
    assert_eq!(attached() - before, 1);
}

#[test]
fn timeout_and_max_silent_time_reach_the_builder() {
    farm_ready();
    let before = failures("TimedOut");
    for (flag, tag) in [("--timeout", "tmot"), ("--max-silent-time", "tmom")] {
        let t0 = Instant::now();
        let o = run_t(
            CLIENT,
            &[
                "nix",
                "build",
                "--store",
                &envoy(),
                "--eval-store",
                "auto",
                "-f",
                &expr("SLOW"),
                "--argstr",
                "tag",
                tag,
                flag,
                "5",
            ],
            120,
        );
        let out = o.combined();
        assert!(
            o.code != 0 && out.contains("timed out") && t0.elapsed().as_secs() < 60,
            "{flag} {} {out}",
            o.code
        );
    }
    assert!(building().is_empty());
    assert_eq!(failures("TimedOut") - before, 2);
}

#[test]
fn a_failing_build_is_counted_with_its_own_reason() {
    farm_ready();
    let failed = || failures("PermanentFailure");
    let build_failed = || {
        WORKERS
            .into_iter()
            .map(|w| events(w, "build_failed"))
            .sum::<i64>()
    };
    let (before, events_before) = (failed(), build_failed());
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "-f",
            &expr("FAIL"),
            "--argstr",
            "tag",
            "boom",
        ],
        120,
    );
    assert!(
        o.code != 0 && o.combined().contains("BOOM-boom"),
        "{}",
        o.combined()
    );
    assert_eq!(failed() - before, 1);
    assert_eq!(build_failed() - events_before, 1);
}

#[test]
fn the_first_client_leaving_does_not_fail_the_second() {
    farm_ready();
    spawn_build("share1", "share");
    sleep(1);
    spawn_build("share2", "share");
    wait_building();
    sleep(2);
    succeed(CLIENT, &["systemctl", "kill", "-s", "INT", "share1"]);
    wait_unit_done("share1", 30);
    wait_building();
    release_slow();
    wait_unit_done("share2", 120);
    assert_unit_success("share2");
}

#[test]
fn upload_whose_reference_the_worker_lacks_is_completed_from_the_cache() {
    farm_ready();
    let dep = expr("DEP");
    let sys = system();
    let nix_build = |attr: &str| {
        succeed(
            CLIENT,
            &[
                "nix-build",
                "--no-out-link",
                &dep,
                "-A",
                attr,
                "--argstr",
                "tag",
                "viacache",
            ],
        )
        .trim()
        .to_string()
    };
    let (rf, referrer) = (nix_build("input"), nix_build("referrer"));
    succeed(
        CLIENT,
        &[
            "sh",
            "-c",
            r#"nix-store --export "$1" > /tmp/shared/ref.closure"#,
            "sh",
            &rf,
        ],
    );
    succeed(
        WORKER1,
        &["sh", "-c", "nix-store --import < /tmp/shared/ref.closure"],
    );
    succeed(WORKER2, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_members(&sys, &[WORKER1]);
    succeed(CLIENT, &["nix", "path-info", "--store", &envoy(), &rf]);
    fail(WORKER2, &["test", "-e", &rf]);
    succeed(WORKER2, &["systemctl", "start", "nix-grpc-daemon.service"]);
    succeed(WORKER1, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_members(&sys, &[WORKER2]);
    succeed(
        CLIENT,
        &[
            "nix",
            "copy",
            "--no-check-sigs",
            "--to",
            &envoy(),
            &referrer,
        ],
    );
    succeed(WORKER2, &["test", "-e", &rf]);
    succeed(WORKER2, &["test", "-e", &referrer]);
    succeed(WORKER1, &["systemctl", "start", "nix-grpc-daemon.service"]);
    wait_members(&sys, &WORKERS);
}

#[test]
fn a_niks3_restart_does_not_cost_the_leader_its_role() {
    farm_ready();
    let was = leader();
    let yields = || journal_count(was, UNIT, "event=scheduler_yield");
    let before = yields();
    succeed(LB, &["systemctl", "restart", "niks3.service"]);
    wait_for_unit(LB, "niks3.service");
    retry(60, "scheduler settled", settled);
    assert_eq!(leader(), was);
    assert_eq!(yields(), before);
    build(&envoy(), "after-niks3-restart", &[]);
}

#[test]
fn scheduler_down_the_other_node_takes_the_lock_and_keeps_it() {
    farm_ready();
    let (was, nxt) = (leader(), standby());
    let pattern = "event=scheduler_take_over";
    let before = journal_count(nxt, UNIT, pattern);
    succeed(was, &["systemctl", "stop", "nix-grpc-daemon.service"]);
    wait_journal_above(nxt, UNIT, pattern, before, 30);
    retry(30, "one worker on the new scheduler", || {
        sched_workers(nxt) == 1
    });
    build(&envoy(), "on-standby", &[]);
    succeed(was, &["systemctl", "start", "nix-grpc-daemon.service"]);
    retry(60, "scheduler settled", settled);
    assert_eq!(leader(), nxt);
    assert_eq!(sched_workers(was), 0);
}

#[test]
fn clean_scheduler_restart_peers_are_told_and_running_builds_survive() {
    farm_ready();
    spawn_build("rs", "rs");
    wait_building();
    let who = building();
    let (was, nxt) = (leader(), standby());
    let pattern = "event=scheduler_restarting addr=";
    let before = journal_count(nxt, UNIT, pattern);
    // --no-block: a build on the leader drains first, so the restart only
    // completes after release_slow().
    succeed(
        was,
        &[
            "systemctl",
            "restart",
            "--no-block",
            "nix-grpc-daemon.service",
        ],
    );
    sleep(5);
    assert_eq!(building(), who, "restart killed or moved the build");
    release_slow();
    wait_journal_above(nxt, UNIT, pattern, before, 60);
    wait_unit_done("rs", 120);
    assert_unit_success("rs");
    assert_no_journal(CLIENT, "rs", "reconnecting");
    retry(60, "scheduler settled", settled);
}

#[test]
fn scheduler_restart_mid_build_finishes_without_a_double_build() {
    farm_ready();
    spawn_build("mid", "mid");
    wait_building();
    assert_eq!(building().len(), 1, "{:?}", building());
    // The scheduler forgets the build. If it ran on the leader it dies with
    // the daemon. Either way no second copy may start while one is alive.
    let ldr = leader();
    succeed(
        ldr,
        &["systemctl", "kill", "-s", "KILL", "nix-grpc-daemon.service"],
    );
    succeed(ldr, &["systemctl", "start", "nix-grpc-daemon.service"]);
    sleep(5);
    assert!(building().len() <= 1, "{:?}", building());
    release_slow();
    wait_unit_done("mid", 120);
    assert_unit_success("mid");
}

#[test]
fn interrupting_the_client_stops_the_build_on_the_worker() {
    farm_ready();
    spawn_build("intr", "intr");
    wait_building();
    succeed(CLIENT, &["systemctl", "kill", "-s", "INT", "intr"]);
    retry(20, "the build stopped", || building().is_empty());
}

#[test]
fn a_deploy_mid_build_drains_and_the_new_generation_takes_over() {
    farm_ready();
    spawn_build("sw", "sw");
    wait_building();
    let busy = if building() == [WORKER1] {
        WORKER1
    } else {
        WORKER2
    };
    if busy == WORKER2 {
        succeed(
            WORKER2,
            &[
                "timeout",
                "120",
                "/run/current-system/specialisation/next/bin/switch-to-configuration",
                "test",
            ],
        );
    } else {
        succeed(WORKER1, &["systemctl", "reload", "nix-grpc-daemon.service"]);
    }
    succeed(busy, BUILDER);
    build(&envoy(), "during-drain", &[]);
    succeed(busy, &["pkill", "-f", "read -t [9]0 x"]);
    wait_unit_done("sw", 90);
    assert_unit_success("sw");
    wait_until_succeeds(
        busy,
        &[
            "sh",
            "-c",
            "systemctl is-active nix-grpc-daemon.service || systemctl start nix-grpc-daemon.service",
        ],
        60,
    );
    wait_members(&system(), &WORKERS);
    retry(90, "scheduler settled", settled);
}

#[test]
fn queue_gauges_show_a_burst_while_every_build_blocks() {
    farm_ready();
    let sys = system();
    let queued = || gauge_sum(leader(), r#"kind="queued""#);
    let slots = gauge_sum(leader(), r#"kind="slots""#);
    let tags = (0..slots + 2)
        .map(|i| format!("\"q{i}\""))
        .collect::<Vec<_>>()
        .join(" ");
    let burst = format!(
        "map (tag: import {} {{ inherit tag; }}) [ {tags} ]",
        expr("SLOW")
    );
    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "burst",
            "nix",
            "build",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "--impure",
            "--expr",
            &burst,
        ],
    );
    // The Wants arrive within a second and nothing else happens until a build
    // ends, so only the trailing export can show the queue.
    retry(10, "two builds queued", || queued() == 2);
    retry(90, "burst finished", || {
        release_slow();
        run_t(CLIENT, &["systemctl", "is-active", "burst"], 60).code != 0
    });
    assert_unit_success("burst");
    retry(10, "queue empty", || queued() == 0);
    let waits = gauge(
        leader(),
        &format!(r#"nix_grpc_queue_seconds_count{{system="{sys}"}}"#),
    );
    assert!(waits >= slots + 2, "{waits}");
}

fn has_vip_build_info(line: &str) -> bool {
    line.starts_with("nix_grpc_build_info{")
        && line.ends_with("} 1")
        && line.contains(r#"worker="node-a""#)
        && line
            .split_once(r#"features=""#)
            .is_some_and(|(_, rest)| rest.split('"').next().unwrap_or("").contains("vip"))
}

#[test]
fn required_system_features_are_placed_or_refused() {
    farm_ready();
    let job = expr("JOB");
    let vip = format!(
        r#"map (tag: import {job} {{ inherit tag; features = ["vip"]; }}) ["f1" "f2" "f3"]"#
    );
    let o = run_t(
        CLIENT,
        &[
            "nix",
            "build",
            "-L",
            "--store",
            &envoy(),
            "--eval-store",
            "auto",
            "--expr",
            &vip,
            "--impure",
        ],
        300,
    );
    assert_eq!(o.code, 0, "{}", o.combined());
    let out = o.combined();
    assert!(out.contains("node-a: building "), "{out}");
    assert!(
        out.lines()
            .filter(|l| l.contains(": building"))
            .all(|l| l.contains("node-a")),
        "{out}"
    );
    assert!(
        worker_metrics(WORKER1)
            .iter()
            .any(|l| has_vip_build_info(l))
    );

    succeed(
        CLIENT,
        &[
            "systemd-run",
            "--unit",
            "gpu",
            "nix",
            "build",
            "-L",
            "--store",
            &format!("{}&restart-grace=30", envoy()),
            "--eval-store",
            "auto",
            "-f",
            &job,
            "--argstr",
            "tag",
            "f5",
            "--arg",
            "features",
            r#"["gpu"]"#,
        ],
    );
    let unplaceable = format!(
        r#"nix_grpc_sched_system{{features="gpu",kind="unplaceable",system="{}"}}"#,
        system()
    );
    retry(60, "one unplaceable build", || {
        gauge(leader(), &unplaceable) == 1
    });
    retry(60, "gpu build failed", || {
        unit_result(CLIENT, "gpu") == "exit-code"
    });
    let out = query(CLIENT, &["journalctl", "-u", "gpu", "-o", "cat"]);
    assert!(out.contains("features {gpu}"), "{out}");
    retry(30, "no unplaceable build", || {
        gauge(leader(), &unplaceable) == 0
    });
}

#[test]
fn low_disk_drains_a_worker_and_builds_go_to_the_other() {
    farm_ready();
    let count = |p: &str| journal_count(WORKER1, UNIT, p);
    let (down, up) = (
        "event=unhealthy reason=min_free",
        "event=healthy reason=min_free",
    );
    let (down0, up0) = (count(down), count(up));
    let avail = succeed(WORKER1, &["df", "--output=avail", "-B1", "/nix/store"]);
    let avail: u64 = avail.lines().last().unwrap().trim().parse().unwrap();
    succeed(
        WORKER1,
        &[
            "fallocate",
            "-l",
            &(avail - 100 * 1024 * 1024).to_string(),
            "/nix/.rw-store/fill",
        ],
    );
    wait_journal_above(WORKER1, UNIT, down, down0, 30);
    let top = build(&envoy(), "drain", &[]);
    assert_eq!(holders(&top), [WORKER2]);
    succeed(WORKER1, &["rm", "/nix/.rw-store/fill"]);
    wait_journal_above(WORKER1, UNIT, up, up0, 30);
}
