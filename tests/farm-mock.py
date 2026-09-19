#!/usr/bin/env python3
"""Mock niks3 for the Cache/PushProcess unit tests.

`farm-mock.py serve` answers /api/objects/present and records pushes;
`farm-mock.py push --stdin --server-url URL ...` stands in for
`niks3 push --stdin` and forwards each line to the server so state stays in
one process. A path containing "fail", "hang" or "crash" makes the push fail,
never ack, or kills the push process.
"""

import json
import os
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

ACK_DELAY = float(os.environ.get("FARM_MOCK_HB", "0.2"))


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.pushed: set[str] = set()  # narinfo keys
        self.log: list[dict[str, Any]] = []

    def record(self, **ev: Any) -> None:
        with self.lock:
            self.log.append(ev)


S = State()


def narinfo_key(path: str) -> str:
    base = path.rsplit("/", 1)[-1]
    return base.split("-", 1)[0] + ".narinfo"


class Http(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_: Any) -> None:
        pass

    def raw_body(self) -> bytes:
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def reply(self, code: int, obj: Any = None) -> None:
        body = json.dumps(obj).encode() if obj is not None else b""
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/api/cache-config":
            self.reply(200, {})
        elif self.path == "/_mock/log":
            with S.lock:
                self.reply(200, S.log)
        else:
            self.reply(404)

    def do_POST(self) -> None:
        auth = self.headers.get("Authorization", "")
        if self.path.startswith("/api/") and auth != "Bearer testtoken":
            self.reply(401, {"error": "bad token"})
            return
        body = self.raw_body()
        if self.path == "/_mock/push":
            self.reply(200, push_result(body.decode()))
            return
        req = json.loads(body or b"{}")
        if self.path == "/api/objects/present":
            with S.lock:
                have = [k for k in req.get("keys", []) if k in S.pushed]
            S.record(ev="present", keys=req.get("keys", []))
            self.reply(200, {"present": have})
        else:
            self.reply(404)


def push_result(line: str) -> list[dict[str, Any]]:
    """What `niks3 push --stdin` would print for one input line."""
    req = json.loads(line)
    paths, rid = req["paths"], req.get("id", 0)
    S.record(ev="push", paths=paths, has_claim_token="claim_token" in req)
    if any("hang" in p for p in paths):
        return []
    time.sleep(ACK_DELAY)
    if any("fail" in p for p in paths):
        status, msg = "error", "push failed"
    else:
        status, msg = "ok", ""
        with S.lock:
            S.pushed.update(narinfo_key(p) for p in paths)
    return [{"id": rid, "path": p, "status": status, "message": msg} for p in paths]


def push_stdin(argv: list[str]) -> None:
    url = argv[argv.index("--server-url") + 1]
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        if "crash" in line:
            os._exit(3)
        req = urllib.request.Request(f"{url}/_mock/push", data=line.encode(), method="POST")
        with urllib.request.urlopen(req) as resp:
            for r in json.load(resp):
                print(json.dumps(r), flush=True)


def main() -> None:
    if sys.argv[1] == "push":
        push_stdin(sys.argv[2:])
        return
    http = ThreadingHTTPServer(("127.0.0.1", 0), Http)
    http.daemon_threads = True
    print(http.server_address[1], flush=True)
    http.serve_forever()


if __name__ == "__main__":
    main()
