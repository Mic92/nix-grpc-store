//! Runs the harness' commands on a test VM. systemd starts one per connection
//! with the vsock connection as stdin and stdout, see tests/lib/e2e-agent.nix.
mod wire;

use std::io::{self, Read};
use std::sync::mpsc;
use std::{process, thread};

fn main() -> io::Result<()> {
    let argv = wire::read_args(&mut io::stdin())?;
    let (acks, acked) = mpsc::channel();
    thread::spawn(move || {
        while let Ok(1) = io::stdin().read(&mut [0u8; 1]) {
            let _ = acks.send(());
        }
        // The harness hung up, it has given up on the command. Exiting makes
        // systemd kill what is left of it.
        process::exit(1);
    });
    // A login shell only for the environment a user would have: the arguments
    // pass through it untouched.
    let login = [
        "/run/current-system/sw/bin/bash",
        "-lc",
        r#"exec "$@""#,
        "bash",
    ];
    wire::serve(&login, &argv, &mut io::stdout(), || {
        acked.recv().map_err(io::Error::other)
    })
}
