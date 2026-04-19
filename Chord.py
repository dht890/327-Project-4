# chord.py
from __future__ import annotations

import argparse
import hashlib
import random
import threading
import time
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any

import Pyro5.api as pyro
from Pyro5.server import expose, oneway


# ----------------------------
# Utilities
# ----------------------------

def sha1_int(s: str) -> int:
    return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16)

def in_interval(x: int, a: int, b: int, mod: int, left_open: bool = True, right_closed: bool = True) -> bool:
    """
    Return True if x is in (a, b] on a ring of size mod (default Chord interval).
    Handles wrap-around.
    """
    if a == b:
        return True

    def _in_linear(xv: int, av: int, bv: int) -> bool:
        left_ok = (xv > av) if left_open else (xv >= av)
        right_ok = (xv <= bv) if right_closed else (xv < bv)
        return left_ok and right_ok

    if a < b:
        return _in_linear(x, a, b)
    else:
        # wrap-around: (a, mod-1] U [0, b]
        return _in_linear(x, a, mod - 1) or _in_linear(x, 0, b)

@dataclass(frozen=True)
class NodeInfo:
    node_id: int
    host: str
    port: int

    @property
    def name(self) -> str:
        # Name server registration key
        return f"chord.node.{self.node_id}"

    @property
    def addr(self) -> str:
        return f"{self.host}:{self.port}"


def node_id_for(host: str, port: int, m: int) -> int:
    return sha1_int(f"{host}:{port}") % (2 ** m)


def proxy_for(node_id: int) -> pyro.Proxy:
    # Node is registered as PYRONAME:chord.node.<id>
    return pyro.Proxy(f"PYRONAME:chord.node.{node_id}")


# ----------------------------
# Chord Node as a Remote Object (Pyro)
# ----------------------------

@expose
class ChordNode:
    """
    Remote-object Chord node (skeleton).

    Methods exposed for remote invocation:
      - ping
      - get_node_info
      - get_successor / set_successor
      - get_predecessor / set_predecessor
      - find_successor(key)
      - closest_preceding_finger(key)
      - notify(node_info)
    """

    def __init__(self, info: NodeInfo, m: int) -> None:
        self.info = info
        self.m = m
        self.ring_size = 2 ** m

        self._lock = threading.RLock()

        self.successor: NodeInfo = info
        self.predecessor: Optional[NodeInfo] = None

        # finger[i] = successor of (n + 2^i) mod 2^m, i in [0..m-1]
        self.fingers: List[NodeInfo] = [info for _ in range(m)]
        self._next_finger = 0

        self._stop = threading.Event()

    # ---------- Exposed helpers ----------

    def ping(self) -> str:
        return "pong"

    def get_node_info(self) -> Dict[str, Any]:
        return {"node_id": self.info.node_id, "host": self.info.host, "port": self.info.port}

    def get_successor(self) -> Dict[str, Any]:
        with self._lock:
            s = self.successor
        return {"node_id": s.node_id, "host": s.host, "port": s.port}

    def get_predecessor(self) -> Optional[Dict[str, Any]]:
        with self._lock:
            p = self.predecessor
        if p is None:
            return None
        return {"node_id": p.node_id, "host": p.host, "port": p.port}

    def set_successor(self, node: Dict[str, Any]) -> None:
        with self._lock:
            self.successor = NodeInfo(node["node_id"], node["host"], node["port"])

    def set_predecessor(self, node: Optional[Dict[str, Any]]) -> None:
        with self._lock:
            if node is None:
                self.predecessor = None
            else:
                self.predecessor = NodeInfo(node["node_id"], node["host"], node["port"])

    # ---------- Chord RPCs ----------

    def find_successor(self, key: int, hops: int = 0):
        """
        Logarithmic Chord lookup (fixed).
        """

        # 🚨 prevent runaway recursion
        if hops > 8:
            with self._lock:
                s = self.successor
            return {
                "node_id": s.node_id,
                "host": s.host,
                "port": s.port
            }

        with self._lock:
            n = self.info
            s = self.successor

        # safety fallback
        if s is None:
            return {
                "node_id": n.node_id,
                "host": n.host,
                "port": n.port
            }

        # if key in (n, successor]
        if in_interval(
            key,
            n.node_id,
            s.node_id,
            self.ring_size,
            left_open=True,
            right_closed=True
        ):
            return {
                "node_id": s.node_id,
                "host": s.host,
                "port": s.port
            }

        # 🔥 REAL FIX: use finger table properly
        cp = self._closest_preceding_finger_local(key)

        # safety fallback
        if cp is None:
            cp = s

        with proxy_for(cp.node_id) as p:
            return p.find_successor(key, hops + 1)

    def _closest_preceding_finger_local(self, key: int):
        """
        Return closest preceding finger in identifier space.
        Scans from highest to lowest for logarithmic routing.
        """

        with self._lock:
            n_id = self.info.node_id
            fingers = self.fingers[:]

        # scan backwards for best match
        for finger in reversed(fingers):
            if finger is None:
                continue

            if in_interval(
                finger.node_id,
                n_id,
                key,
                self.ring_size,
                left_open=True,
                right_open=True
            ):
                return finger

        # fallback
        return self.successor

    def notify(self, node: Dict[str, Any]) -> None:
        """
        n'.notify(n): n' thinks it might be your predecessor.
        """
        cand = NodeInfo(node["node_id"], node["host"], node["port"])
        with self._lock:
            if self.predecessor is None:
                self.predecessor = cand
                return

            p = self.predecessor
            # If cand in (p, self] then update predecessor
            if in_interval(cand.node_id, p.node_id, self.info.node_id, self.ring_size, left_open=True, right_closed=False):
                self.predecessor = cand

    # ---------- Local helpers (not remote) ----------

    def _closest_preceding_finger_local(self, key: int) -> NodeInfo:
        with self._lock:
            for i in reversed(range(self.m)):
                f = self.fingers[i]
                # want f in (n, key)
                if in_interval(f.node_id, self.info.node_id, key, self.ring_size, left_open=True, right_closed=False):
                    return f
            return self.info

    # ----------------------------
    # Maintenance (stabilize/fix_fingers/check_predecessor)
    # ----------------------------

    @oneway
    def start_maintenance(self, stabilize_period: float = 1.0, fix_fingers_period: float = 1.0) -> None:
        """
        Start background threads (oneway so caller doesn't block).
        """
        threading.Thread(target=self._stabilize_loop, args=(stabilize_period,), daemon=True).start()
        threading.Thread(target=self._fix_fingers_loop, args=(fix_fingers_period,), daemon=True).start()

    @oneway
    def stop(self) -> None:
        self._stop.set()

    def _stabilize_loop(self, period: float) -> None:
        while not self._stop.is_set():
            try:
                self.stabilize()
            except Exception:
                pass
            time.sleep(period)

    def _fix_fingers_loop(self, period: float) -> None:
        while not self._stop.is_set():
            try:
                self.fix_fingers()
            except Exception:
                pass
            time.sleep(period)

    def stabilize(self) -> None:
        """
        Standard Chord stabilize:
          x = successor.predecessor
          if x in (n, successor) then successor = x
          successor.notify(n)
        """
        with self._lock:
            s = self.successor
            n = self.info

        # Ask successor for its predecessor
        try:
            with proxy_for(s.node_id) as ps:
                x_dict = ps.get_predecessor()
        except Exception:
            return

        if x_dict is not None:
            x = NodeInfo(x_dict["node_id"], x_dict["host"], x_dict["port"])
            # if x in (n, s) then update successor to x
            if in_interval(x.node_id, n.node_id, s.node_id, self.ring_size, left_open=True, right_closed=False):
                with self._lock:
                    self.successor = x
                    self.fingers[0] = x
                s = x

        # Notify successor
        try:
            with proxy_for(s.node_id) as ps:
                ps.notify({"node_id": n.node_id, "host": n.host, "port": n.port})
        except Exception:
            return

    def fix_fingers(self) -> None:
        """
        Periodically refresh one finger entry.
        """
        with self._lock:
            i = self._next_finger
            self._next_finger = (self._next_finger + 1) % self.m
            start = (self.info.node_id + (1 << i)) % self.ring_size

        succ_dict = self.find_successor(start)
        succ = NodeInfo(succ_dict["node_id"], succ_dict["host"], succ_dict["port"])
        with self._lock:
            self.fingers[i] = succ
            if i == 0:
                self.successor = succ

    # ----------------------------
    # Ring operations
    # ----------------------------

    def create(self) -> None:
        with self._lock:
            self.predecessor = None
            self.successor = self.info
            self.fingers = [self.info for _ in range(self.m)]

    def join(self, known: Dict[str, Any]) -> None:
        """
        Join the ring using a known node.
        Initializes successor + FULL finger table (critical fix).
        """

        known_id = known["node_id"]

        # --- find successor of ourselves ---
        with proxy_for(known_id) as pk:
            succ_dict = pk.find_successor(self.info.node_id)

        succ = NodeInfo(succ_dict["node_id"], succ_dict["host"], succ_dict["port"])

        with self._lock:
            self.successor = succ
            self.predecessor = None

            # FIX: initialize ALL fingers, not just finger[0]
            for i in range(len(self.fingers)):
                self.fingers[i] = succ


# ----------------------------
# Process runner
# ----------------------------

def run_node(host: str, port: int, m: int, bootstrap: bool, join_addr: Optional[str]) -> None:
    nid = node_id_for(host, port, m)
    info = NodeInfo(nid, host, port)
    node = ChordNode(info, m)

    # Locate Name Server
    ns = pyro.locate_ns()

    with pyro.Daemon(host=host, port=port) as daemon:
        uri = daemon.register(node)
        ns.register(info.name, uri)
        print(f"[node {nid}] registered {info.name} -> {uri}")

        if bootstrap:
            node.create()
            print(f"[node {nid}] created new ring (successor=self)")
        else:
            if not join_addr:
                raise SystemExit("Must supply --join host:port when not bootstrapping")
            jh, jp = join_addr.split(":")
            jp_i = int(jp)
            jid = node_id_for(jh, jp_i, m)
            node.join({"node_id": jid, "host": jh, "port": jp_i})
            print(f"[node {nid}] joined via node {jid}; successor={node.successor.node_id}")

        # Start maintenance loops
        node.start_maintenance(stabilize_period=1.0, fix_fingers_period=1.0)

        print(f"[node {nid}] running; ctrl-c to stop")
        daemon.requestLoop()


def run_lookup(start_addr: str, m: int, key: int) -> None:
    sh, sp = start_addr.split(":")
    sp_i = int(sp)
    sid = node_id_for(sh, sp_i, m)
    with proxy_for(sid) as p:
        res = p.find_successor(key)
    print(f"[lookup] start={sid} key={key} -> successor={res['node_id']} ({res['host']}:{res['port']})")


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    ap_node = sub.add_parser("node")
    ap_node.add_argument("--host", default="127.0.0.1")
    ap_node.add_argument("--port", type=int, required=True)
    ap_node.add_argument("--m", type=int, default=8)
    ap_node.add_argument("--bootstrap", action="store_true")
    ap_node.add_argument("--join", dest="join_addr", default=None, help="host:port of known node")

    ap_lookup = sub.add_parser("lookup")
    ap_lookup.add_argument("--start", required=True, help="host:port of starting node")
    ap_lookup.add_argument("--m", type=int, default=8)
    ap_lookup.add_argument("--key", type=int, required=True)

    args = ap.parse_args()

    if args.cmd == "node":
        run_node(args.host, args.port, args.m, args.bootstrap, args.join_addr)
    else:
        run_lookup(args.start, args.m, args.key)


if __name__ == "__main__":
    main()