//! Floating content-addressed derivations through the build hook on a
//! single node without a cache: the client fetches their outputs from the
//! builder, which holds them for the client's session once it knows them.
//! A GC on the node takes such an output in the gaps the builder leaves,
//! and a build trace the GC left behind meets a rebuild.

use crate::fixtures::*;
use crate::harness::{MACHINE, journal, unit_result};

const EXPR: &str = "/tmp/floating.nix";
/// The client's own store, so that only the builder's /nix/store has what
/// the builder made.
const CLIENT_STORE: &str = "/root/floating-client";
const UNIT: &str = "nix-grpc-daemon-mtls.service";

/// What finds `tag`'s slow build, and no other test's.
fn slow(tag: &str) -> String {
    format!("read -t [9]0 {tag} ")
}

fn strs(args: &[String]) -> Vec<&str> {
    args.iter().map(String::as_str).collect()
}

fn write_expr() {
    write_file(EXPR, include_str!("ca.nix"));
}

/// `nix build` of `attr` for `tag` into `store` through the build hook and
/// the mTLS node, as argv, with `params` for the hook's store.
fn build_argv(store: &str, attr: &str, tag: &str, params: &str) -> Vec<String> {
    let sys = succeed(&["nix", "config", "show", "system"])
        .trim()
        .to_string();
    let builders = format!(
        "{}{params} {sys} - 1 1 ca-derivations",
        cert_store("client")
    );
    [
        "nix",
        "build",
        "-L",
        "--print-out-paths",
        "--no-link",
        "--store",
        store,
        "--max-jobs",
        "0",
        "--builders",
        &builders,
        "-f",
        EXPR,
        attr,
        "--argstr",
        "tag",
        tag,
    ]
    .map(String::from)
    .into()
}

/// Starts `tag`'s slow build into `store` as `unit`.
fn spawn(unit: &str, store: &str, tag: &str) {
    let mut cmd = Vec::from(["systemd-run", "--unit", unit].map(String::from));
    cmd.extend(build_argv(store, "slow", tag, ""));
    succeed(&strs(&cmd));
}

/// Starts `tag`'s slow build into `store` as `unit` and waits until it runs.
fn spawn_slow(unit: &str, store: &str, tag: &str) {
    spawn(unit, store, tag);
    wait_until_succeeds(&["pgrep", "-f", &slow(tag)], 120);
}

/// Lets `tag`'s slow build, built again unless the client gave up or got
/// it already, end, and checks that `unit`'s client got `out` into `store`.
fn finish_slow(unit: &str, store: &str, tag: &str, out: &str) {
    wait_until_succeeds(
        &[
            "sh",
            "-c",
            r#"pgrep -f "$1" || ! systemctl is-active -q "$2""#,
            "sh",
            &slow(tag),
            unit,
        ],
        120,
    );
    run_t(&["pkill", "-f", &slow(tag)], 60);
    wait_until_succeeds(
        &["sh", "-c", r#"! systemctl is-active -q "$1""#, "sh", unit],
        120,
    );
    assert_eq!(
        unit_result(MACHINE, unit),
        "success",
        "{}",
        journal(MACHINE, unit)
    );
    assert_eq!(
        succeed(&["nix", "store", "cat", "--store", store, out]).trim(),
        format!("slow-{tag}")
    );
}

fn events(kind: &str) -> i64 {
    let name = format!(r#"nix_grpc_events_total{{kind="{kind}"}} "#);
    crate::harness::metrics(MACHINE, 9464)
        .iter()
        .find_map(|l| l.strip_prefix(&name))
        .map_or(0, |v| v.trim().parse::<f64>().unwrap_or(0.0) as i64)
}

/// Builds the scheduler gave back after a transient failure.
fn given_back() -> i64 {
    events("given_back_TRANSIENT_FAILURE")
}

/// The path of a floating output named `name` that holds the line `line`.
fn ca_path(name: &str, line: &str) -> String {
    write_file("/tmp/floating-line", line);
    succeed(&[
        "nix",
        "store",
        "add",
        "--dry-run",
        "--mode",
        "nar",
        "--name",
        name,
        "/tmp/floating-line",
    ])
    .trim()
    .to_string()
}

/// The live nix-daemon connections, in the order they were opened, whose
/// temp roots name `named` and no derivation unless `named` is one, other
/// than the one that runs `tag`'s slow build.
fn connections(tag: &str, named: &str) -> Vec<String> {
    succeed(&[
        "sh",
        "-c",
        r#"b=$(pgrep -f "$1" | head -n1); anc=" "; p=${b:-1}
while [ "$p" -gt 1 ]; do anc="$anc$p "; p=$(awk '/^PPid:/{print $2}' /proc/$p/status); done
for f in $(grep -lF -e "$2" /nix/var/nix/temproots/*); do pid=${f##*/}
  [ -d "/proc/$pid" ] || continue
  case "$anc" in *" $pid "*) continue;; esac
  case "$2" in *.drv) ;; *) grep -qF .drv "$f" && continue;; esac
  echo "$pid"
done | sort -n"#,
        "sh",
        &slow(tag),
        named,
    ])
    .split_whitespace()
    .map(String::from)
    .collect()
}

/// nix-daemon connections, stopped until this is dropped.
struct Stopped(Vec<String>);

impl Stopped {
    fn new(pids: Vec<String>) -> Self {
        assert!(!pids.is_empty(), "no connection to stop");
        let stopped = Stopped(pids);
        for pid in &stopped.0 {
            succeed(&["kill", "-STOP", pid]);
        }
        stopped
    }

    /// Waits until a request sits unread at one of them.
    fn wait_request(&self) {
        let mut argv = vec![
            "sh",
            "-c",
            r#"for p; do ss -xpn | awk -v p="pid=$p," 'index($0, p) && $3 > 0 {f=1} END {exit !f}' && exit 0; done; exit 1"#,
            "sh",
        ];
        argv.extend(self.0.iter().map(String::as_str));
        wait_until_succeeds(&argv, 120);
    }
}

impl Drop for Stopped {
    fn drop(&mut self) {
        for pid in &self.0 {
            run_t(&["kill", "-CONT", pid], 60);
        }
    }
}

/// nix-daemon does not root a floating output, so the builder roots it on
/// the build RPC's root connection and then checks it is still there. With
/// that connection stopped, the root waits, and the GC takes the output first:
/// the builder gives the build back, which the scheduler records, and its
/// build trace still names that path, so a reproducible build is made
/// again here.
#[test]
fn a_floating_output_the_gc_took_before_it_was_rooted_is_given_back() {
    wait_for_unit(UNIT);
    write_expr();
    let out = ca_path("floating-slow-gc", "slow-gc");
    let dep = succeed(&[
        "nix",
        "eval",
        "--raw",
        "-f",
        EXPR,
        "dep.outPath",
        "--argstr",
        "tag",
        "gc",
    ]);
    let before = given_back();
    spawn_slow("floating-gc", CLIENT_STORE, "gc");
    {
        // The build RPC's root connection roots its input, and names no
        // derivation.
        let stopped = Stopped::new(connections("gc", &dep));
        succeed(&["pkill", "-f", &slow("gc")]);
        stopped.wait_request();
        succeed(&["nix", "store", "delete", &out]);
    }
    finish_slow("floating-gc", CLIENT_STORE, "gc", &out);
    assert_eq!(given_back(), before + 1);
    assert_journal("floating-gc", "was garbage-collected before it was rooted");
}

/// The builder holds a floating output for the client's session, over that
/// session's own connection, while the build RPC's root connection still
/// roots it: with the session's connection stopped, the hold waits, and the GC
/// still cannot take the output.
#[test]
fn a_floating_output_is_rooted_until_it_is_held() {
    wait_for_unit(UNIT);
    write_expr();
    let out = ca_path("floating-slow-gap", "slow-gap");
    spawn_slow("floating-gap", CLIENT_STORE, "gap");
    {
        let stopped = Stopped::new(connections("gap", "-floating-slow-gap.drv"));
        succeed(&["pkill", "-f", &slow("gap")]);
        stopped.wait_request();
        let gc = run_t(&["nix", "store", "delete", &out], 60);
        assert_ne!(gc.code, 0, "the GC took {out}: {}", gc.combined());
        assert!(gc.combined().contains("still alive"), "{}", gc.combined());
    }
    finish_slow("floating-gap", CLIENT_STORE, "gap", &out);
}

/// Two clients build the same floating derivation at once: the second
/// attaches to the first's build, and both get its output, built once.
#[test]
fn a_second_client_attaches_to_a_floating_build() {
    wait_for_unit(UNIT);
    write_expr();
    let out = ca_path("floating-slow-attach", "slow-attach");
    let other = "/root/floating-client-attach";
    let attached = events("attached");
    spawn_slow("floating-attach-a", CLIENT_STORE, "attach");
    spawn("floating-attach-b", other, "attach");
    crate::harness::retry(60, "the second client attached", || {
        events("attached") > attached
    });
    // Both clients have their input by now: what is built from here on is
    // the slow build, or a rebuild of it.
    let built = events("built");
    finish_slow("floating-attach-a", CLIENT_STORE, "attach", &out);
    finish_slow("floating-attach-b", other, "attach", &out);
    assert_eq!(events("built"), built + 1);
}

/// A second client attached to the build learns its floating output only
/// once the owner is done, and the owner's session alone holds it then. With
/// that session's connection gone and the second's stopped, the GC takes
/// the output before the second's hold: that client is told to ask again,
/// not handed a path the builder lost.
#[test]
fn a_floating_output_the_gc_took_before_an_attached_client_held_it_is_built_again() {
    wait_for_unit(UNIT);
    write_expr();
    let out = ca_path("floating-slow-share", "slow-share");
    let other = "/root/floating-client-b";
    let attached = events("attached");
    spawn_slow("floating-share-a", CLIENT_STORE, "share");
    spawn("floating-share-b", other, "share");
    crate::harness::retry(60, "the second client attached", || {
        events("attached") > attached
    });
    // Each session's connection, the owner's opened first.
    let sessions = connections("share", "-floating-slow-share.drv");
    assert_eq!(sessions.len(), 2, "{sessions:?}");
    {
        let stopped = Stopped::new(vec![sessions[1].clone()]);
        succeed(&["pkill", "-f", &slow("share")]);
        stopped.wait_request();
        wait_until_succeeds(
            &["sh", "-c", "! systemctl is-active -q floating-share-a"],
            120,
        );
        assert_eq!(unit_result(MACHINE, "floating-share-a"), "success");
        succeed(&["kill", "-KILL", &sessions[0]]);
        wait_until_succeeds(&["nix", "store", "delete", &out], 120);
    }
    finish_slow("floating-share-b", other, "share", &out);
    assert_journal(
        "floating-share-b",
        "was garbage-collected before it was held",
    );
}

/// The GC takes an output whose build is not reproducible, and keeps its
/// build trace. A rebuild here makes other bytes, which a nix-daemon 2.35
/// through current master refuses: the builder gives the build back, so
/// that another builder may make it. Deleting the trace lets it through.
#[test]
fn a_rebuild_a_stale_build_trace_refuses_is_given_back() {
    wait_for_unit(UNIT);
    write_expr();
    let first = succeed(&strs(&build_argv(CLIENT_STORE, "nondet", "stale", "")));
    // Its session holds it: that session's connection goes first.
    for pid in connections("stale", "-floating-nondet-stale.drv") {
        succeed(&["kill", "-KILL", &pid]);
    }
    wait_until_succeeds(&["nix", "store", "delete", first.trim()], 120);
    let before = given_back();
    // A client without the realisation asks again. With one builder it
    // gives up after asking once more.
    let again = build_argv(
        "/root/floating-client-stale",
        "nondet",
        "stale",
        "&reschedule-retries=0",
    );
    let o = run_t(&strs(&again), 300);
    assert_ne!(o.code, 0, "{}", o.combined());
    assert!(
        o.combined().contains("names an output it no longer has"),
        "{}",
        o.combined()
    );
    assert!(given_back() > before);
    let drv = succeed(&[
        "nix",
        "eval",
        "--raw",
        "-f",
        EXPR,
        "nondet.drvPath",
        "--argstr",
        "tag",
        "stale",
    ]);
    succeed(&[
        "nix",
        "store",
        "build-trace",
        "delete",
        "--store",
        "local",
        &format!("{}^out", drv.trim()),
    ]);
    succeed(&strs(&build_argv(
        "/root/floating-client-stale-2",
        "nondet",
        "stale",
        "",
    )));
}
