use crate::fixtures::*;
use crate::harness::{MACHINE, journal_count};

const INPUT_MIB: usize = 1024;
const BUILD_TIMEOUT_S: u64 = 120;

struct Outcome {
    code: i32,
    started: u64,
    finished: u64,
    log: String,
}

const DAEMON: &str = "nix-grpc-daemon";

fn uploads(event: &str) -> u64 {
    journal_count(
        MACHINE,
        DAEMON,
        &format!("{event} method=AddMultipleToStore"),
    )
}

/// Builds `jobs` derivations that share one large input through Envoy.
fn build_sharing_one_input(name: &str, jobs: usize) -> Outcome {
    wait_for_unit("envoy.service");
    wait_for_open_port(50060);
    let dir = workdir(name);
    let big = format!("{dir}/big");
    succeed(&[
        "dd",
        "if=/dev/urandom",
        &format!("of={big}"),
        "bs=1M",
        &format!("count={INPUT_MIB}"),
        "status=none",
    ]);
    write_file(
        &format!("{dir}/jobs.nix"),
        &format!(
            r#"let big = builtins.path {{ path = {big}; name = "big"; }}; in
builtins.genList (i: derivation {{
  name = "job-${{toString i}}";
  system = builtins.currentSystem;
  builder = "/bin/sh";
  args = [ "-c" "echo ok > $out" ];
  inherit big;
  tag = toString i;
}}) {jobs}"#
        ),
    );
    let src = format!("local?root={dir}/src");
    let jobs_nix = format!("{dir}/jobs.nix");
    let targets: Vec<String> = (0..jobs)
        .map(|i| {
            let drv = succeed(&[
                "nix-instantiate",
                "--store",
                &src,
                &jobs_nix,
                "-A",
                &i.to_string(),
            ]);
            format!("{}^out", drv.trim())
        })
        .collect();

    let (started0, finished0) = (uploads("event=rpc_start"), uploads("event=rpc"));
    let timeout = BUILD_TIMEOUT_S.to_string();
    let mut build = vec![
        "timeout",
        "-s",
        "KILL",
        &timeout,
        "nix",
        "build",
        "--store",
        ENVOY_STORE,
        "--eval-store",
        &src,
        "--no-link",
    ];
    build.extend(targets.iter().map(String::as_str));
    let o = run_t(&build, BUILD_TIMEOUT_S + 60);
    let output = o.combined();
    let tail: Vec<&str> = output.lines().rev().take(20).collect();
    succeed(&["rm", "-rf", &big, &format!("{dir}/src")]);
    Outcome {
        code: o.code,
        started: uploads("event=rpc_start") - started0,
        finished: uploads("event=rpc") - finished0,
        log: tail.into_iter().rev().collect::<Vec<_>>().join("\n"),
    }
}

impl Outcome {
    fn assert_finished(&self, what: &str) {
        assert_eq!(
            self.code, 0,
            "{what} did not finish within {BUILD_TIMEOUT_S}s: {} uploads started, {} finished\n{}",
            self.started, self.finished, self.log
        );
    }
}

#[test]
fn a_single_build_of_the_large_input_finishes() {
    build_sharing_one_input("upload-one", 1).assert_finished("one build");
}

#[test]
fn concurrent_builds_sharing_the_large_input_finish() {
    build_sharing_one_input("upload-many", 16).assert_finished("16 builds");
}
