use std::env as std_env;
use std::io::{self, Read};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::sync::{Mutex, Once};
use std::thread;
use std::time::{Duration, Instant};

use crate::wire;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Node(pub &'static str);

pub const MACHINE: Node = Node("machine");

pub fn env(key: &str) -> String {
    std_env::var(key).unwrap_or_else(|_| panic!("missing env var {key}"))
}

pub struct Out {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    /// The connection to the node failed and the command never reported its
    /// status, so we do not know whether it ran to the end.
    pub lost: bool,
    /// The node took the command, so it may have run.
    started: bool,
}

impl Out {
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

/// Reads from a stream until a deadline, however many reads that takes.
struct Until<'a> {
    stream: &'a UnixStream,
    deadline: Instant,
}

impl Read for Until<'_> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let left = self.deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Err(io::ErrorKind::TimedOut.into());
        }
        self.stream.set_read_timeout(Some(left))?;
        let mut stream = self.stream;
        stream.read(buf)
    }
}

/// Asks vhost-device-vsock, which listens on `sock` for one VM, for the
/// agent's port on that VM.
fn connect(sock: &Path, deadline: Instant) -> io::Result<UnixStream> {
    use std::io::Write;
    let stream = UnixStream::connect(sock)?;
    (&stream).write_all(format!("CONNECT {}\n", wire::PORT).as_bytes())?;
    let mut answer = Vec::new();
    let mut byte = [0u8; 1];
    let mut from = Until {
        stream: &stream,
        deadline,
    };
    while byte[0] != b'\n' {
        from.read_exact(&mut byte)?;
        answer.push(byte[0]);
    }
    if !answer.starts_with(b"OK ") {
        return Err(io::Error::other(format!(
            "no agent: {}",
            String::from_utf8_lossy(&answer).trim()
        )));
    }
    Ok(stream)
}

fn exchange(sock: &Path, argv: &[&str], timeout: Duration) -> Out {
    let deadline = Instant::now() + timeout;
    let mut started = false;
    let mut output = wire::Output::default();
    let code = (|| {
        let stream = connect(sock, deadline)?;
        started = true;
        wire::write_args(&mut &stream, argv)?;
        let mut from = Until {
            stream: &stream,
            deadline,
        };
        wire::receive(&mut from, &mut &stream, &mut output)
    })();
    let timed_out = code.as_ref().is_err_and(|e| {
        matches!(
            e.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        )
    });
    let mut stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    if let Err(e) = &code {
        stderr.push_str(&e.to_string());
    }
    Out {
        code: code.as_ref().map_or(-1, |c| *c),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr,
        timed_out,
        lost: code.is_err() && !timed_out,
        started,
    }
}

fn run_once(node: Node, argv: &[&str], timeout: Duration) -> Out {
    let sock = env(&format!("NGS_{}_SOCK", node.0.to_uppercase()));
    exchange(Path::new(&sock), argv, timeout)
}

static READY: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
static HEARTBEAT: Once = Once::new();

fn ensure_ready(node: Node) {
    HEARTBEAT.call_once(|| {
        let start = Instant::now();
        thread::spawn(move || {
            loop {
                thread::sleep(Duration::from_secs(30));
                println!("[e2e] heartbeat t={}s", start.elapsed().as_secs());
            }
        });
    });
    let mut ready = READY.lock().unwrap_or_else(|e| e.into_inner());
    if ready.contains(&node.0) {
        return;
    }
    let deadline = Instant::now() + Duration::from_secs(120);
    while run_once(node, &["true"], Duration::from_secs(20)).code != 0 {
        assert!(
            Instant::now() < deadline,
            "agent on {} never became ready",
            node.0
        );
        thread::sleep(Duration::from_millis(500));
    }
    ready.push(node.0);
}

/// Runs `attempt` again while the connection fails. A repeatable command is
/// retried even if it may have started; any other only if the node never took
/// it, so it cannot run twice.
fn retrying(repeatable: bool, mut attempt: impl FnMut() -> Out) -> Out {
    let mut o = attempt();
    for _ in 0..4 {
        if o.timed_out || !o.lost || !(repeatable || !o.started) {
            break;
        }
        thread::sleep(Duration::from_secs(2));
        o = attempt();
    }
    o
}

/// `argv` for failure messages. The node gets the arguments themselves.
fn shell_join(argv: &[&str]) -> String {
    argv.join(" ")
}

fn run(node: Node, argv: &[&str], secs: u64, repeatable: bool) -> Out {
    let cmd = shell_join(argv);
    ensure_ready(node);
    let o = retrying(repeatable, || {
        run_once(node, argv, Duration::from_secs(secs))
    });
    assert!(
        !o.timed_out,
        "[{}] timed out after {secs}s: {cmd}\n{}",
        node.0,
        o.combined()
    );
    assert!(
        !o.lost,
        "[{}] connection lost: {cmd}\n{}",
        node.0,
        o.combined()
    );
    o
}

pub fn run_t(node: Node, argv: &[&str], secs: u64) -> Out {
    run(node, argv, secs, false)
}

fn expect_ok(node: Node, argv: &[&str], repeatable: bool) -> String {
    let cmd = shell_join(argv);
    let o = run(node, argv, 300, repeatable);
    assert_eq!(
        o.code,
        0,
        "[{}] failed ({}): {cmd}\n{}",
        node.0,
        o.code,
        o.combined()
    );
    o.stdout
}

pub fn succeed(node: Node, argv: &[&str]) -> String {
    expect_ok(node, argv, false)
}

/// Like `succeed`, for read-only commands such as `journalctl` or `systemctl
/// show`, which are safe to repeat after a dropped connection.
pub fn query(node: Node, argv: &[&str]) -> String {
    expect_ok(node, argv, true)
}

pub fn fail(node: Node, argv: &[&str]) -> String {
    let cmd = shell_join(argv);
    let o = run_t(node, argv, 300);
    assert_ne!(
        o.code,
        0,
        "[{}] expected failure: {cmd}\n{}",
        node.0,
        o.combined()
    );
    o.combined()
}

pub fn wait_until_succeeds(node: Node, argv: &[&str], secs: u64) {
    let cmd = shell_join(argv);
    let deadline = Instant::now() + Duration::from_secs(secs);
    loop {
        let o = run(node, argv, 60, true);
        if o.code == 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "[{}] not true after {secs}s: {cmd}\nlast output (exit {}): {}",
            node.0,
            o.code,
            o.combined()
        );
        thread::sleep(Duration::from_millis(500));
    }
}

pub fn wait_for_unit(node: Node, unit: &str) {
    wait_until_succeeds(node, &["systemctl", "is-active", unit], 120);
}

pub fn wait_for_open_port(node: Node, port: u16) {
    wait_until_succeeds(
        node,
        &["bash", "-c", &format!(": < /dev/tcp/127.0.0.1/{port}")],
        60,
    );
}

pub fn unit_state(node: Node, unit: &str) -> String {
    query(node, &["systemctl", "show", "-P", "ActiveState", unit])
        .trim()
        .to_string()
}

pub fn journal(node: Node, unit: &str) -> String {
    query(node, &["journalctl", "-u", unit, "--no-pager"])
}

/// Whether `line` matches `pattern`, where `.*` stands for any text. That is
/// all the patterns in these tests use of a regular expression.
fn line_matches(line: &str, pattern: &str) -> bool {
    let mut rest = line;
    for part in pattern.split(".*") {
        match rest.find(part) {
            Some(at) => rest = &rest[at + part.len()..],
            None => return false,
        }
    }
    true
}

/// `pattern` as a regular expression for `journalctl --grep`.
fn grep_pattern(pattern: &str) -> String {
    pattern
        .split(".*")
        .map(|part| {
            part.chars()
                .map(|c| {
                    if r".[]()*+?{}|^$\".contains(c) {
                        format!("\\{c}")
                    } else {
                        c.to_string()
                    }
                })
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join(".*")
}

/// Filters on the node, so a long journal need not cross to the host.
pub fn journal_matches(node: Node, unit: &str, pattern: &str) -> Vec<String> {
    let regex = grep_pattern(pattern);
    let argv = [
        "journalctl",
        "-u",
        unit,
        "--no-pager",
        "--case-sensitive=true",
        "--grep",
        &regex,
    ];
    let o = run(node, &argv, 300, true);
    // 1: no line matched.
    assert!(
        o.code <= 1,
        "[{}] failed ({}): journalctl\n{}",
        node.0,
        o.code,
        o.combined()
    );
    o.stdout
        .lines()
        .filter(|l| line_matches(l, pattern))
        .map(String::from)
        .collect()
}

pub fn journal_count(node: Node, unit: &str, pattern: &str) -> u64 {
    journal_matches(node, unit, pattern).len() as u64
}

pub fn assert_journal(node: Node, unit: &str, pattern: &str) {
    if journal_matches(node, unit, pattern).is_empty() {
        panic!(
            "[{}] journal of {unit} has no line matching: {pattern}\n{}",
            node.0,
            journal(node, unit)
        );
    }
}

pub fn assert_no_journal(node: Node, unit: &str, pattern: &str) {
    let hits: Vec<_> = journal_matches(node, unit, pattern);
    assert!(
        hits.is_empty(),
        "[{}] journal of {unit} has lines matching: {pattern}\n{}",
        node.0,
        hits.join("\n")
    );
}

pub fn wait_journal_above(node: Node, unit: &str, pattern: &str, before: u64, secs: u64) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while journal_count(node, unit, pattern) <= before {
        assert!(
            Instant::now() < deadline,
            "[{}] journal of {unit} never got a new line matching: {pattern}",
            node.0
        );
        thread::sleep(Duration::from_millis(500));
    }
}

pub fn write_file(node: Node, path: &str, content: &str) {
    succeed(
        node,
        &[
            "sh",
            "-c",
            r#"printf '%s\n' "$1" > "$2""#,
            "write",
            content,
            path,
        ],
    );
}

pub fn retry(secs: u64, what: &str, mut f: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(secs);
    while !f() {
        assert!(Instant::now() < deadline, "not true after {secs}s: {what}");
        thread::sleep(Duration::from_millis(500));
    }
}

pub fn sleep(secs: u64) {
    thread::sleep(Duration::from_secs(secs));
}

pub fn metrics(node: Node, port: u16) -> Vec<String> {
    query(
        node,
        &["curl", "-sf", &format!("http://127.0.0.1:{port}/metrics")],
    )
    .lines()
    .map(String::from)
    .collect()
}

pub fn unit_result(node: Node, unit: &str) -> String {
    query(
        node,
        &["systemctl", "show", "-p", "Result", "--value", unit],
    )
    .trim()
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn out(lost: bool, started: bool) -> Out {
        Out {
            code: if lost { -1 } else { 0 },
            stdout: String::new(),
            stderr: String::new(),
            timed_out: false,
            lost,
            started,
        }
    }

    fn calls(repeatable: bool, started: bool) -> u32 {
        let mut n = 0;
        retrying(repeatable, || {
            n += 1;
            out(n < 3, started)
        });
        n
    }

    #[test]
    fn a_repeatable_command_is_retried_after_any_drop() {
        assert_eq!(calls(true, true), 3);
    }

    #[test]
    fn another_command_is_retried_only_if_the_node_never_took_it() {
        assert_eq!(calls(false, true), 1);
        assert_eq!(calls(false, false), 3);
    }

    /// Stands in for vhost-device-vsock and, if `agent`, the agent behind it.
    fn node(agent: bool) -> PathBuf {
        static N: AtomicU32 = AtomicU32::new(0);
        let sock = std::env::temp_dir().join(format!(
            "ngs-{}-{}.sock",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let listener = UnixListener::bind(&sock).unwrap();
        thread::spawn(move || {
            for stream in listener.incoming() {
                let mut stream = stream.unwrap();
                thread::spawn(move || {
                    let mut hello = String::new();
                    BufReader::new(&stream).read_line(&mut hello).unwrap();
                    assert_eq!(hello, format!("CONNECT {}\n", wire::PORT));
                    if !agent {
                        return;
                    }
                    stream.write_all(b"OK 1073741824\n").unwrap();
                    let argv = wire::read_args(&mut stream).unwrap();
                    let mut acks = stream.try_clone().unwrap();
                    let _ = wire::serve(&[], &argv, &mut stream, || acks.read_exact(&mut [0u8; 1]));
                });
            }
        });
        sock
    }

    #[test]
    fn a_command_reports_its_status_and_both_streams() {
        let o = exchange(
            &node(true),
            &["bash", "-c", "echo out; echo err >&2; exit 3"],
            Duration::from_secs(10),
        );
        assert_eq!(
            (o.code, o.stdout.as_str(), o.stderr.as_str()),
            (3, "out\n", "err\n")
        );
        assert!(!o.lost && !o.timed_out);
    }

    #[test]
    fn output_larger_than_one_piece_arrives_whole() {
        let argv = ["head", "-c", "300000", "/dev/zero"];
        let o = exchange(&node(true), &argv, Duration::from_secs(10));
        assert_eq!((o.code, o.stdout.len()), (0, 300000));
    }

    #[test]
    fn arguments_reach_the_command_untouched() {
        let argv = ["printf", "<%s>", "a b", "", "it's", "$HOME", "x;y\n*"];
        let o = exchange(&node(true), &argv, Duration::from_secs(10));
        assert_eq!(o.stdout, "<a b><><it's><$HOME><x;y\n*>");
    }

    #[test]
    fn a_command_killed_by_a_signal_reports_it_like_a_shell() {
        let o = exchange(
            &node(true),
            &["bash", "-c", "kill -KILL $$"],
            Duration::from_secs(10),
        );
        assert_eq!(o.code, 137);
        assert!(!o.lost);
    }

    #[test]
    fn a_command_that_outlives_its_timeout_is_not_waited_for() {
        let start = Instant::now();
        let argv = ["bash", "-c", "echo started; sleep 30"];
        let o = exchange(&node(true), &argv, Duration::from_millis(500));
        assert!(o.timed_out && !o.lost);
        assert_eq!(o.stdout, "started\n");
        assert!(start.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn a_node_without_an_agent_never_took_the_command() {
        let o = exchange(&node(false), &["true"], Duration::from_secs(10));
        assert!(o.lost && !o.started && !o.timed_out, "{}", o.stderr);
    }

    #[test]
    fn a_pattern_matches_text_with_a_wildcard_between_its_parts() {
        let line = "event=denied method=Connect cn=stranger role=none";
        assert!(line_matches(
            line,
            "event=denied method=.* cn=stranger role=none"
        ));
        assert!(line_matches(line, "cn=stranger"));
        assert!(!line_matches(line, "cn=stranger.*event=denied"));
        assert!(!line_matches(line, "role=read-only"));
    }
}
