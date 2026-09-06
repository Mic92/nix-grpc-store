#!/usr/bin/env python3
"""Mock niks3 for farm client tests.

`farm-mock.py serve` is the HTTP server: cache-config, NDJSON claim stream
with heartbeats, fail, plus /_mock control endpoints to inject events and
read what the mock saw. `farm-mock.py push --stdin --server-url URL ...`
stands in for `niks3 push --stdin` and forwards each line to the server so
state stays in one process. A path containing "fail", "stale" or "crash"
makes the push fail, go stale, or kills the push process.
"""

import json
import os
import select
import socket
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

HB = float(os.environ.get("FARM_MOCK_HB", "0.2"))


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.next_token = 1
        self.claims: dict[str, int] = {}  # key -> token
        self.built: set[str] = set()
        self.failed: dict[str, str] = {}  # key -> kind, one-shot for current waiters
        self.drop: set[str] = set()  # keys whose holder stream should be cut
        self.frozen = False  # niks3 "down": streams cut, new claims hang silently
        self.overload = 0  # next N claim requests get 503 Retry-After
        self.log: list[dict[str, Any]] = []

    def record(self, **ev: Any) -> None:
        with self.lock:
            self.log.append(ev)


S = State()


def key_of(outputs: list[str]) -> str:
    return "\n".join(sorted(outputs))


class Http(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_: Any) -> None:
        pass

    def raw_body(self) -> bytes:
        return self.rfile.read(int(self.headers.get("Content-Length", "0")))

    def reply(self, code: int, obj: Any = None) -> None:
        data = b"" if obj is None else json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        if self.path == "/api/cache-config":
            self.reply(200, {"claim_heartbeat_secs": HB})
        elif self.path == "/_mock/log":
            with S.lock:
                self.reply(200, S.log)
        else:
            self.reply(404)

    def do_POST(self) -> None:
        auth = self.headers.get("Authorization", "")
        if self.path.startswith("/api/") and auth != "Bearer testtoken":
            self.reply(401)
            return
        raw = self.raw_body()
        if self.path == "/_mock/push":
            self.reply(200, push_result(raw.decode().strip()))
            return
        req = json.loads(raw or b"{}")
        if self.path == "/api/builds/claim":
            self.claim(req)
        elif self.path == "/api/builds/fail":
            self.fail(req)
        elif self.path == "/_mock/complete":
            with S.lock:
                k = key_of(req["outputs"])
                S.claims.pop(k, None)
                S.built.add(k)
                S.cv.notify_all()
            self.reply(204)
        elif self.path == "/_mock/overload":
            with S.lock:
                S.overload = req["n"]
            self.reply(204)
        elif self.path == "/_mock/freeze":
            with S.lock:
                S.frozen = req.get("frozen", True)
                S.cv.notify_all()
            self.reply(204)
        elif self.path == "/_mock/drop":
            with S.lock:
                S.drop.add(key_of(req["outputs"]))
                S.cv.notify_all()
            self.reply(204)
        else:
            self.reply(404)

    def fail(self, req: dict[str, Any]) -> None:
        with S.lock:
            k = next((k for k, t in S.claims.items() if t == req["claim_token"]), None)
            if k is None:
                self.reply(409)
                return
            del S.claims[k]
            if req.get("kind"):
                S.failed[k] = req["kind"]
            S.cv.notify_all()
        S.record(ev="fail", token=req["claim_token"], kind=req.get("kind", ""))
        self.reply(204)

    def send_line(self, obj: dict[str, Any]) -> None:
        line = json.dumps(obj).encode() + b"\n"
        self.wfile.write(b"%x\r\n%s\r\n" % (len(line), line))
        self.wfile.flush()

    def claim(self, req: dict[str, Any]) -> None:
        k = key_of(req["outputs"])
        S.record(ev="claim", outputs=req["outputs"], token=req.get("token", 0))
        if S.frozen:
            time.sleep(10 * HB)
            self.reply(503)
            return
        with S.lock:
            shed = S.overload > 0
            S.overload -= shed
        if shed:
            S.record(ev="shed")
            self.send_response(503)
            self.send_header("Retry-After", "1")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        sent_wait = False
        try:
            while True:
                with S.lock:
                    if k in S.built:
                        st: dict[str, Any] | None = {"status": "built"}
                    elif k in S.failed:
                        st = {"status": "failed", "kind": S.failed.pop(k)}
                    elif k not in S.claims or S.claims[k] == req.get("token"):
                        tok = S.claims.get(k) or S.next_token
                        if k not in S.claims:
                            S.next_token += 1
                            S.claims[k] = tok
                        st = {"status": "build", "token": tok}
                    else:
                        st = None
                if st is not None:
                    self.send_line(st)
                    if st["status"] == "build":
                        self.hold(k, st["token"])
                    break
                if not sent_wait:
                    self.send_line({"status": "wait"})
                    sent_wait = True
                with S.lock:
                    S.cv.wait(HB)
                self.send_line({"status": "hb"})
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            try:
                self.wfile.write(b"0\r\n\r\n")
                self.wfile.flush()
            except OSError:
                pass
            self.close_connection = True

    def hold(self, k: str, token: int) -> None:
        """Heartbeat until the client goes away, the claim is released, or the test drops us."""
        while True:
            with S.lock:
                if S.claims.get(k) != token:
                    return
                if k in S.drop or S.frozen:
                    S.drop.discard(k)
                    S.log.append({"ev": "dropped", "token": token})
                    self.connection.shutdown(socket.SHUT_RDWR)
                    return
            r, _, _ = select.select([self.connection], [], [], HB)
            try:
                if r and self.connection.recv(1, socket.MSG_PEEK) == b"":
                    break
                self.send_line({"status": "hb"})
            except OSError:
                break
        with S.lock:
            if S.claims.get(k) == token:
                del S.claims[k]
                S.log.append({"ev": "released_by_disconnect", "token": token})
                S.cv.notify_all()


def push_result(line: str) -> list[dict[str, Any]]:
    """What `niks3 push --stdin` would print for one input line."""
    if line.startswith("{"):
        req = json.loads(line)
        paths, tok = req["paths"], req.get("claim_token", 0)
    else:
        paths, tok = [line], 0
    S.record(ev="push", paths=paths, claim_token=tok)
    time.sleep(HB)
    if any("fail" in p for p in paths):
        status, msg = "error", "push failed"
    elif any("stale" in p for p in paths):
        status, msg = "stale", "stale build claim"
    else:
        status, msg = "ok", ""
        with S.lock:
            k = next((k for k, t in S.claims.items() if t == tok), None)
            if k is not None:
                del S.claims[k]
                S.built.add(k)
                S.cv.notify_all()
    return [{"path": p, "status": status, "message": msg} for p in paths]


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
