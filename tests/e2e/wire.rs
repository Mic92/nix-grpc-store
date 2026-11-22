//! What the harness and the agent on a test VM say to each other: the harness
//! sends a command's arguments, the agent answers with its exit status,
//! stdout and stderr.
#![allow(dead_code)]

use std::ffi::OsStr;
use std::io::{self, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, SyncSender};
use std::thread;

/// The vsock port the agent listens on, as in tests/lib/e2e-agent.nix.
pub const PORT: u32 = 4000;

pub fn write_frame(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    let len = u32::try_from(data.len()).map_err(io::Error::other)?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(data)
}

pub fn read_frame(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len)?;
    let mut data = vec![0u8; u32::from_be_bytes(len) as usize];
    r.read_exact(&mut data)?;
    Ok(data)
}

pub fn write_args(w: &mut impl Write, argv: &[&str]) -> io::Result<()> {
    write_frame(w, &(argv.len() as u32).to_be_bytes())?;
    argv.iter().try_for_each(|a| write_frame(w, a.as_bytes()))
}

pub fn read_args(r: &mut impl Read) -> io::Result<Vec<Vec<u8>>> {
    let count: [u8; 4] = read_frame(r)?
        .try_into()
        .map_err(|_| io::Error::other("bad argument count"))?;
    (0..u32::from_be_bytes(count))
        .map(|_| read_frame(r))
        .collect()
}

const EXIT: u8 = 0;
const STDOUT: u8 = 1;
const STDERR: u8 = 2;

// vhost-device-vsock stalls on a write the guest has no credit left for.
// Small pieces, each acknowledged, keep the guest's view of its credit fresh.
const PIECE: usize = 16 * 1024;

fn pump<R: Read + Send + 'static>(stream: u8, mut from: R, to: SyncSender<(u8, Vec<u8>)>) {
    thread::spawn(move || {
        let mut buf = vec![0u8; PIECE];
        while let Ok(n @ 1..) = from.read(&mut buf) {
            if to.send((stream, buf[..n].to_vec())).is_err() {
                break;
            }
        }
    });
}

/// Runs `argv`, behind `wrapper` if there is one, and sends what it prints as
/// it comes, then its exit status. `ack` waits for the peer to confirm a piece.
pub fn serve(
    wrapper: &[&str],
    argv: &[Vec<u8>],
    to: &mut impl Write,
    mut ack: impl FnMut() -> io::Result<()>,
) -> io::Result<()> {
    let mut words = wrapper
        .iter()
        .map(OsStr::new)
        .chain(argv.iter().map(|a| OsStr::from_bytes(a)));
    let program = words.next().ok_or_else(|| io::Error::other("no command"))?;
    let mut child = Command::new(program)
        .args(words)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let (pieces, queue) = mpsc::sync_channel(4);
    pump(STDOUT, child.stdout.take().unwrap(), pieces.clone());
    pump(STDERR, child.stderr.take().unwrap(), pieces);
    for (stream, data) in queue {
        to.write_all(&[stream])?;
        write_frame(to, &data)?;
        to.flush()?;
        ack()?;
    }
    let status = child.wait()?;
    // As a shell reports a child that died from a signal.
    let code = status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(0));
    to.write_all(&[EXIT])?;
    to.write_all(&code.to_be_bytes())?;
    to.flush()
}

#[derive(Default)]
pub struct Output {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

/// Collects what a command prints into `output` and returns its exit status.
/// After a failure `output` holds what had arrived.
pub fn receive(from: &mut impl Read, to: &mut impl Write, output: &mut Output) -> io::Result<i32> {
    loop {
        let mut stream = [0u8; 1];
        from.read_exact(&mut stream)?;
        match stream[0] {
            EXIT => {
                let mut code = [0u8; 4];
                from.read_exact(&mut code)?;
                return Ok(i32::from_be_bytes(code));
            }
            STDOUT => output.stdout.extend(read_frame(from)?),
            STDERR => output.stderr.extend(read_frame(from)?),
            other => return Err(io::Error::other(format!("bad reply {other}"))),
        }
        to.write_all(&[0])?;
    }
}
