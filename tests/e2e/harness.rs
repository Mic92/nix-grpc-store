use std::env as std_env;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{Mutex, Once};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Node(pub &'static str);

pub const MACHINE: Node = Node("machine");

pub fn env(key: &str) -> String {
    std_env::var(key).unwrap_or_else(|_| panic!("missing env var {key}"))
}

fn ssh_args(node: Node) -> Vec<String> {
    let mut args = vec!["-F".into(), env("NGS_SSH_CONFIG")];
    for opt in [
        "User=root",
        "StrictHostKeyChecking=no",
        "UserKnownHostsFile=/dev/null",
        "ConnectTimeout=30",
        "ServerAliveInterval=30",
        "ServerAliveCountMax=10",
        "LogLevel=ERROR",
    ] {
        args.push("-o".into());
        args.push(opt.into());
    }
    args.push(format!(
        "vsock-mux/{}",
        env(&format!("NGS_{}_SOCK", node.0.to_uppercase()))
    ));
    args
}

pub struct Out {
    pub code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
    /// ssh itself failed (exit 255) and the command never reported its
    /// status, so we do not know whether it ran to the end. A command that
    /// returns 255 on its own still reports, so it is not mistaken for this.
    pub lost: bool,
}

impl Out {
    pub fn combined(&self) -> String {
        format!("{}{}", self.stdout, self.stderr)
    }
}

fn drain<R: Read + Send + 'static>(mut r: R) -> thread::JoinHandle<String> {
    thread::spawn(move || {
        let mut b = Vec::new();
        let _ = r.read_to_end(&mut b);
        String::from_utf8_lossy(&b).into_owned()
    })
}

fn run_once(node: Node, cmd: &str, timeout: Duration) -> Out {
    let mut child = Command::new(env("NGS_SSH"))
        .args(ssh_args(node))
        .arg(format!(
            "( set -euo pipefail\n{cmd}\n)\necho \"{RC_MARK}$?\"\n"
        ))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn ssh");
    // Readers keep a chatty command from filling the pipe and blocking.
    let out = drain(child.stdout.take().unwrap());
    let err = drain(child.stderr.take().unwrap());

    let deadline = Instant::now() + timeout;
    let mut timed_out = false;
    let status = loop {
        if let Some(s) = child.try_wait().expect("try_wait") {
            break s;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            timed_out = true;
            break child.wait().expect("wait after kill");
        }
        thread::sleep(Duration::from_millis(100));
    };
    let mut stdout = out.join().unwrap();
    // ssh exits 255 when the remote command dies from a signal, so the
    // command's own status travels in the output.
    let (code, lost) = match stdout.rfind(RC_MARK) {
        Some(at) => {
            let rc = stdout[at + RC_MARK.len()..].trim().parse().unwrap_or(-1);
            stdout.truncate(at);
            (rc, false)
        }
        None => (status.code().unwrap_or(-1), status.code() == Some(255)),
    };
    Out {
        code,
        stdout,
        stderr: err.join().unwrap(),
        timed_out,
        lost,
    }
}

const RC_MARK: &str = "__NGS_RC=";

fn connect_failed(stderr: &str) -> bool {
    [
        "banner exchange",
        "Connection refused",
        "Connection timed out",
        "Connection closed by remote host",
        "kex_exchange_identification",
    ]
    .iter()
    .any(|m| stderr.contains(m))
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
    while run_once(node, "true", Duration::from_secs(20)).code != 0 {
        assert!(
            Instant::now() < deadline,
            "ssh backdoor of {} never became ready",
            node.0
        );
        thread::sleep(Duration::from_millis(500));
    }
    ready.push(node.0);
}

/// Runs `attempt` again while ssh fails. A repeatable command is retried even
/// if it may have started; any other only if ssh could not connect, so it
/// cannot run twice.
fn retrying(repeatable: bool, mut attempt: impl FnMut() -> Out) -> Out {
    let mut o = attempt();
    for _ in 0..4 {
        if o.timed_out || !o.lost || !(repeatable || connect_failed(&o.stderr)) {
            break;
        }
        thread::sleep(Duration::from_secs(2));
        o = attempt();
    }
    o
}

/// Quotes `arg` for the remote shell, so it arrives as one word.
fn quote(arg: &str) -> String {
    let plain = !arg.is_empty()
        && arg
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_./:=@%+,".contains(c));
    if plain {
        arg.to_string()
    } else {
        format!("'{}'", arg.replace('\'', r"'\''"))
    }
}

/// The remote command line for `argv`. ssh hands it to the login shell.
fn shell_join(argv: &[&str]) -> String {
    argv.iter().map(|a| quote(a)).collect::<Vec<_>>().join(" ")
}

fn run(node: Node, argv: &[&str], secs: u64, repeatable: bool) -> Out {
    let cmd = shell_join(argv);
    let cmd = cmd.as_str();
    ensure_ready(node);
    let o = retrying(repeatable, || {
        run_once(node, cmd, Duration::from_secs(secs))
    });
    assert!(
        !o.timed_out,
        "[{}] timed out after {secs}s: {cmd}\n{}",
        node.0,
        o.combined()
    );
    assert!(
        !o.lost,
        "[{}] ssh connection lost: {cmd}\n{}",
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

pub fn journal_matches(node: Node, unit: &str, pattern: &str) -> Vec<String> {
    journal(node, unit)
        .lines()
        .filter(|l| line_matches(l, pattern))
        .map(String::from)
        .collect()
}

pub fn journal_count(node: Node, unit: &str, pattern: &str) -> u64 {
    journal_matches(node, unit, pattern).len() as u64
}

pub fn assert_journal(node: Node, unit: &str, pattern: &str) {
    let log = journal(node, unit);
    assert!(
        log.lines().any(|l| line_matches(l, pattern)),
        "[{}] journal of {unit} has no line matching: {pattern}\n{log}",
        node.0
    );
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

    fn out(lost: bool, stderr: &str) -> Out {
        Out {
            code: if lost { 255 } else { 0 },
            stdout: String::new(),
            stderr: stderr.into(),
            timed_out: false,
            lost,
        }
    }

    fn calls(repeatable: bool, stderr: &str) -> u32 {
        let mut n = 0;
        retrying(repeatable, || {
            n += 1;
            out(n < 3, stderr)
        });
        n
    }

    #[test]
    fn a_repeatable_command_is_retried_after_any_drop() {
        assert_eq!(calls(true, "Connection reset by peer"), 3);
    }

    #[test]
    fn another_command_is_retried_only_if_ssh_never_connected() {
        assert_eq!(calls(false, "Connection reset by peer"), 1);
        assert_eq!(calls(false, "Connection refused"), 3);
    }

    #[test]
    fn arguments_reach_the_remote_shell_as_single_words() {
        assert_eq!(shell_join(&["ls", "-l", "/nix/store"]), "ls -l /nix/store");
        assert_eq!(
            shell_join(&["echo", "a b", "", "it's", "$HOME", "x;y"]),
            r#"echo 'a b' '' 'it'\''s' '$HOME' 'x;y'"#
        );
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
