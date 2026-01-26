#!/usr/bin/env python3
"""
Enhanced Participant for 2PC / 3PC
Adds:
✔ durable WAL (fsync)
✔ crash recovery (replay)
✔ READY timeout (shows blocking in 2PC)
✔ detailed logs
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import json
import threading
import time
import os
from typing import Dict, Any, Optional

lock = threading.Lock()

NODE_ID: str = ""
PORT: int = 8001

kv: Dict[str, str] = {}
TX: Dict[str, Dict[str, Any]] = {}

WAL_PATH: Optional[str] = None
READY_TIMEOUT = 15   # seconds (for demo blocking)

# ---------------------------------------------------
# Utils
# ---------------------------------------------------

def jdump(obj: Any) -> bytes:
    return json.dumps(obj).encode("utf-8")

def jload(b: bytes) -> Any:
    return json.loads(b.decode("utf-8"))

# ---------------------------------------------------
# WAL
# ---------------------------------------------------

def wal_append(line: str) -> None:
    if not WAL_PATH:
        return

    with open(WAL_PATH, "a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")
        f.flush()
        os.fsync(f.fileno())  # 🔥 durability


def wal_replay():
    """Recover state after crash"""
    if not WAL_PATH or not os.path.exists(WAL_PATH):
        return

    print(f"[{NODE_ID}] WAL replay...")

    with open(WAL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split(" ", 2)
            if not parts:
                continue

            txid = parts[0]
            cmd = parts[1]

            if cmd == "PREPARE" or cmd == "CAN_COMMIT":
                vote, op_json = parts[2].split(" ", 1)
                TX[txid] = {
                    "state": "READY" if vote == "YES" else "ABORTED",
                    "op": json.loads(op_json),
                    "ts": time.time()
                }

            elif cmd == "PRECOMMIT":
                TX[txid]["state"] = "PRECOMMIT"

            elif cmd == "COMMIT":
                TX[txid]["state"] = "COMMITTED"
                apply_op(TX[txid]["op"])

            elif cmd == "ABORT":
                TX[txid] = {"state": "ABORTED", "op": None}

    print(f"[{NODE_ID}] WAL recovery finished")


# ---------------------------------------------------
# Operation logic
# ---------------------------------------------------

def validate_op(op: dict) -> bool:
    return op.get("type", "").upper() == "SET"


def apply_op(op: dict) -> None:
    if op["type"].upper() == "SET":
        kv[str(op["key"])] = str(op["value"])


# ---------------------------------------------------
# Timeout monitor (shows blocking)
# ---------------------------------------------------

def timeout_monitor():
    while True:
        time.sleep(2)

        now = time.time()

        with lock:
            for txid, rec in list(TX.items()):
                if rec["state"] == "READY":
                    if now - rec["ts"] > READY_TIMEOUT:
                        print(f"[{NODE_ID}] TX {txid} STILL BLOCKED (2PC limitation)")


# ---------------------------------------------------
# HTTP Handler
# ---------------------------------------------------

class Handler(BaseHTTPRequestHandler):

    def _send(self, code: int, obj: dict):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # -------------------------
    # GET
    # -------------------------

    def do_GET(self):
        if self.path.startswith("/status"):
            with lock:
                self._send(200, {
                    "node": NODE_ID,
                    "kv": kv,
                    "tx": TX
                })
            return

        self._send(404, {"error": "not found"})

    # -------------------------
    # POST
    # -------------------------

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = jload(self.rfile.read(length)) if length else {}

        txid = body.get("txid")

        # -------- PREPARE (2PC) --------
        if self.path == "/prepare":
            op = body["op"]

            vote = "YES" if validate_op(op) else "NO"

            with lock:
                TX[txid] = {
                    "state": "READY" if vote == "YES" else "ABORTED",
                    "op": op,
                    "ts": time.time()
                }

            wal_append(f"{txid} PREPARE {vote} {json.dumps(op)}")

            print(f"[{NODE_ID}] {txid} VOTE-{vote}")

            self._send(200, {"vote": vote})
            return

        # -------- COMMIT --------
        if self.path == "/commit":
            with lock:
                rec = TX.get(txid)
                apply_op(rec["op"])
                rec["state"] = "COMMITTED"

            wal_append(f"{txid} COMMIT")

            print(f"[{NODE_ID}] {txid} COMMIT")

            self._send(200, {"ok": True})
            return

        # -------- ABORT --------
        if self.path == "/abort":
            with lock:
                TX[txid] = {"state": "ABORTED", "op": None}

            wal_append(f"{txid} ABORT")

            print(f"[{NODE_ID}] {txid} ABORT")

            self._send(200, {"ok": True})
            return

        # -------- 3PC --------
        if self.path == "/can_commit":
            op = body["op"]
            vote = "YES" if validate_op(op) else "NO"

            with lock:
                TX[txid] = {"state": "READY", "op": op, "ts": time.time()}

            wal_append(f"{txid} CAN_COMMIT {vote} {json.dumps(op)}")

            self._send(200, {"vote": vote})
            return

        if self.path == "/precommit":
            with lock:
                TX[txid]["state"] = "PRECOMMIT"

            wal_append(f"{txid} PRECOMMIT")

            self._send(200, {"ok": True})
            return

        self._send(404, {"error": "not found"})

    def log_message(self, *args):
        return


# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

def main():
    global NODE_ID, PORT, WAL_PATH

    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True)
    ap.add_argument("--port", type=int, default=8001)
    ap.add_argument("--wal", default="")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal or None

    wal_replay()  # 🔥 recovery

    threading.Thread(target=timeout_monitor, daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)

    print(f"[{NODE_ID}] running on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
