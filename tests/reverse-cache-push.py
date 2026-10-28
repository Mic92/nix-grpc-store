#!/usr/bin/env python3
"""Expose a rooted fixture store to niks3 in a private mount namespace."""

import ctypes
import os
import subprocess
import sys
import threading


def mount(source: str | None, target: str, fstype: str | None, flags: int) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    encode = lambda value: value.encode() if value is not None else None
    if libc.mount(encode(source), encode(target), encode(fstype), flags, None):
        raise OSError(ctypes.get_errno(), f"mount {source} on {target}")


def main() -> None:
    runtime_file, host_store, worker_store, niks3, *args = sys.argv[1:]
    # MS_REC|MS_PRIVATE and MS_BIND. Changes to /nix/store stay in this child.
    mount(None, "/", None, 16384 | 262144)
    os.makedirs(host_store, exist_ok=True)
    mount("/nix/store", host_store, None, 4096)
    mount("tmpfs", "/nix/store", "tmpfs", 0)
    with open(runtime_file) as paths:
        for source in paths:
            source = source.strip()
            if source:
                os.symlink(f"{host_store}/{os.path.basename(source)}", source)
    child = subprocess.Popen([niks3, *args], stdin=subprocess.PIPE, stdout=subprocess.PIPE)

    def forward_acks() -> None:
        assert child.stdout is not None
        for line in child.stdout:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()

    acks = threading.Thread(target=forward_acks)
    acks.start()
    assert child.stdin is not None
    for line in sys.stdin.buffer:
        # PushProcess starts its long-lived child on the first upload. Later
        # imported paths appear only after startup: refresh before each push.
        for name in os.listdir(worker_store):
            target = f"/nix/store/{name}"
            if not os.path.lexists(target):
                source = f"{worker_store}/{name}"
                if os.path.isdir(source):
                    os.mkdir(target)
                elif os.path.isfile(source):
                    open(target, "xb").close()
                else:
                    continue
                # A symlink changes the serialized NAR; bind the actual file
                # or directory so niks3 signs the same NAR the worker built.
                mount(source, target, None, 4096)
        child.stdin.write(line)
        child.stdin.flush()
    child.stdin.close()
    acks.join()
    sys.exit(child.wait())


if __name__ == "__main__":
    main()
