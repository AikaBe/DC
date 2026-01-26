#!/usr/bin/env python3
"""
Enhanced Coordinator for 2PC / 3PC

Adds:
✔ WAL durability
✔ crash recovery
✔ decision replay
✔ retry propagation
✔ clean logs
"""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request
import argparse
import json
import threading
import time
import os
from typing import Dict, Any, List, Tuple, Optional

lock = threading.Lock()

NODE_ID = ""
PORT = 8000
PARTICIPANTS: List[str] = []

TIMEOUT_S = 2.0
RETRY_INTERVAL = 3

TX: Dict[str, Dict[str, Any]] = {}

WAL_PATH: Optional[str] = None

# -------------------------------------------------
# Utils
# -------------------------------------------------

def jdump(obj):
    return json.dumps(obj).encode()

def jload(b):
    return json.loads(b.decode())


# -------------------------------------------------
# WAL
# -------------------------------------------------

def wal_append(line: str):
    if not WAL_PATH:
        return
    with open(WAL_PATH, "a") as f:
        f.write(line + "\n")
        f.flush()
        os.fsync(f.fileno())


def wal_replay():
    if not WAL_PATH or not os.path.exists(WAL_PATH):
        return

    print("[COORD] WAL replay...")

    with open(WAL_PATH) as f:
        for line in f:
            parts = line.strip().split()

            txid = parts[0]
            cmd = parts[1]

            if cmd == "DECISION":
                decision = parts[2]
                TX[txid] = {
                    "txid": txid,
                    "decision": decision,
                    "state": "RECOVERED",
                    "participants": list(PARTICIPANTS)
                }

    print("[COORD] WAL recovery finished")


# -------------------------------------------------
# HTTP helper
# -------------------------------------------------

def post_json(url: str, payload: dict) -> Tuple[int, dict]:
    data = jdump(payload)
    req = request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return resp.status, jload(resp.read())


# -------------------------------------------------
# Retry thread
# -------------------------------------------------

def retry_loop():
    """Retry commit/abort until all participants receive decision"""
    while True:
        time.sleep(RETRY_INTERVAL)

        with lock:
            txs = list(TX.values())

        for rec in txs:
            decision = rec.get("decision")
            if decision not in ("COMMIT", "ABORT"):
                continue

            endpoint = "/commit" if decision == "COMMIT" else "/abort"

            for p in PARTICIPANTS:
                try:
                    post_json(p + endpoint, {"txid": rec["txid"]})
                except:
                    continue


# -------------------------------------------------
# 2PC
# -------------------------------------------------

def two_pc(txid: str, op: dict):

    print(f"[COORD] {txid} PREPARE")

    votes = {}
    all_yes = True

    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p + "/prepare", {"txid": txid, "op": op})
            vote = resp.get("vote", "NO")
            print(f"[COORD] vote from {p} = {vote}")
            votes[p] = vote
            if vote != "YES":
                all_yes = False
        except:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    decision = "COMMIT" if all_yes else "ABORT"

    print(f"[COORD] GLOBAL {decision}")

    wal_append(f"{txid} DECISION {decision}")

    endpoint = "/commit" if decision == "COMMIT" else "/abort"

    for p in PARTICIPANTS:
        try:
            post_json(p + endpoint, {"txid": txid})
        except:
            pass

    with lock:
        TX[txid] = {
            "txid": txid,
            "protocol": "2PC",
            "decision": decision,
            "votes": votes,
            "state": "DONE",
        }

    return {"txid": txid, "decision": decision, "votes": votes}


# -------------------------------------------------
# 3PC
# -------------------------------------------------

def three_pc(txid: str, op: dict):

    print(f"[COORD] {txid} CAN_COMMIT")

    votes = {}
    all_yes = True

    for p in PARTICIPANTS:
        try:
            _, resp = post_json(p + "/can_commit", {"txid": txid, "op": op})
            vote = resp.get("vote", "NO")
            votes[p] = vote
            if vote != "YES":
                all_yes = False
        except:
            votes[p] = "NO_TIMEOUT"
            all_yes = False

    if not all_yes:
        decision = "ABORT"
    else:
        print(f"[COORD] {txid} PRECOMMIT")
        for p in PARTICIPANTS:
            try:
                post_json(p + "/precommit", {"txid": txid})
            except:
                pass
        decision = "COMMIT"

    print(f"[COORD] GLOBAL {decision}")

    wal_append(f"{txid} DECISION {decision}")

    endpoint = "/commit" if decision == "COMMIT" else "/abort"

    for p in PARTICIPANTS:
        try:
            post_json(p + endpoint, {"txid": txid})
        except:
            pass

    with lock:
        TX[txid] = {
            "txid": txid,
            "protocol": "3PC",
            "decision": decision,
            "votes": votes,
            "state": "DONE",
        }

    return {"txid": txid, "decision": decision, "votes": votes}


# -------------------------------------------------
# HTTP Server
# -------------------------------------------------

class Handler(BaseHTTPRequestHandler):

    def _send(self, code, obj):
        data = jdump(obj)
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path.startswith("/status"):
            self._send(200, {"tx": TX})
            return
        self._send(404, {})

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = jload(self.rfile.read(length)) if length else {}

        if self.path == "/tx/start":
            txid = body["txid"]
            op = body["op"]
            protocol = body.get("protocol", "2PC").upper()

            if protocol == "3PC":
                result = three_pc(txid, op)
            else:
                result = two_pc(txid, op)

            self._send(200, result)
            return

        self._send(404, {})

    def log_message(self, *args):
        return


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():
    global NODE_ID, PORT, PARTICIPANTS, WAL_PATH

    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="COORD")
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--participants", required=True)
    ap.add_argument("--wal", default="/tmp/coord.wal")
    args = ap.parse_args()

    NODE_ID = args.id
    PORT = args.port
    WAL_PATH = args.wal
    PARTICIPANTS = [p.strip() for p in args.participants.split(",")]

    wal_replay()

    threading.Thread(target=retry_loop, daemon=True).start()

    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)

    print(f"[COORD] running on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
