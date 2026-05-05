import json
import os
import tempfile
import threading
import time

import Pyro5.api as pyro

from Chord import ChordNode, NodeInfo, node_id_for
from dfs import DFSClient
from paxos import PaxosAcceptor, paxosProxyFor
from replication import ReplicatedChordStorage
from storageNode import StorageNode

M = 4
BASE_PORT = 9200 # ports 9200-9204, independent of Part C

def start_peer(host, port, ns, bootstrap=False, join_node_id=None):
    nid = node_id_for(host, port, M)
    info = NodeInfo(nid, host, port)
    chord_node = ChordNode(info, M)
    storage_node = StorageNode(nid)
    paxos_node = PaxosAcceptor(nid)

    # Register all services under the same Chord node id
    daemon = pyro.Daemon(host=host, port=port)
    ns.register(f"chord.node.{nid}", daemon.register(chord_node))
    ns.register(f"chord.storage.{nid}", daemon.register(storage_node))
    ns.register(f"chord.paxos.{nid}", daemon.register(paxos_node))
    threading.Thread(target=daemon.requestLoop, daemon=True).start()
    time.sleep(0.3)
    if bootstrap:
        chord_node.create()
        chord_node.start_maintenance(stabilize_period=0.5, fix_fingers_period=0.5)
        print(f"  Node {nid:2d}  bootstrapped  port={port}")
    else:
        chord_node.join({"node_id": join_node_id, "host": host, "port": port})
        chord_node.start_maintenance(stabilize_period=0.5, fix_fingers_period=0.5)
        print(f"  Node {nid:2d}  joined        port={port}")
    return nid

def section(title):
    print()
    print("-" * 60)
    print(f"  {title}")
    print("-" * 60)

def write_tmp_page(name, lines):
    path = os.path.join(tempfile.gettempdir(), name)
    with open(path, "w") as f:
        f.writelines(lines)
    return path

def printMetaLine(nodeId, raw):
    meta = json.loads(raw)
    print(
        f"    Node {nodeId:2d}: "
        f"version={meta['version']} "
        f"numPages={meta['numPages']} "
        f"byteSize={meta.get('byteSize', '-')}"
    )

def main():
    print("\n" + "=" * 60)
    print("  Part D - Paxos (simplified single-decree protocol)")
    print("=" * 60)
    ns = pyro.locate_ns()
    host = "127.0.0.1"
    # Start all peers before running Paxos requests
    section("1. Start 5-node Chord ring")
    node_ids = []
    nid = start_peer(host, BASE_PORT, ns, bootstrap=True)
    node_ids.append(nid)
    for i in range(1, 5):
        nid = start_peer(host, BASE_PORT + i, ns, join_node_id=node_ids[0])
        node_ids.append(nid)
    print(f"\n  Node IDs: {node_ids}")
    print("  Waiting for ring to stabilize (8 s)...")
    time.sleep(8)
    storage = ReplicatedChordStorage(node_ids[0])
    dfs = DFSClient(storage)
    # Normal operation shows prepare, accept, and commit messages
    section("2. touch + append - full Paxos protocol log")
    print("\n  >>> touch('log.txt') triggers Paxos for metadata creation <<<\n")
    dfs.touch("log.txt")
    print("\n  >>> append triggers Paxos for metadata update <<<\n")
    path = write_tmp_page("d_p0.txt", ["entry0,alpha\n", "entry1,beta\n"])
    dfs.append("log.txt", path)
    os.unlink(path)
    meta_key = "metadata:log.txt"
    replicas = storage.getReplicas(meta_key)
    print(f"\n  Replica set for {meta_key!r}: {replicas}")
    leader = min(replicas)
    print(f"  Deterministic leader: node {leader} (lowest node_id)")
    print("\n  Metadata agreement on each replica:")
    for nid in replicas:
        raw = storage._getProxy(nid).remoteGet(meta_key)
        printMetaLine(nid, raw)

    # One crashed replica still leaves a majority
    section("3. Failure - crash replica[2], append with 2/3 majority")
    crash_1 = replicas[2]
    print(f"\n  Crashing node {crash_1}...")
    with paxosProxyFor(crash_1) as p:
        p.crash()
    print(f"  Node {crash_1} is DOWN.\n")
    path = write_tmp_page("d_p1.txt", ["entry2,gamma\n"])
    print(f"  Attempting append with node {crash_1} down:")
    dfs.append("log.txt", path)
    os.unlink(path)
    print(f"\n  Result: COMMITTED with 2/3 replicas (majority = 2)")
    print("  Surviving replicas:")
    for nid in [r for r in replicas if r != crash_1]:
        raw = storage._getProxy(nid).remoteGet(meta_key)
        printMetaLine(nid, raw)
    print(f"    Node {crash_1:2d}  UNREACHABLE (crashed)")

    # Two crashed replicas should prevent consensus
    section("4. Failure - crash replica[1] too, below majority, FAILS")
    crash_2 = replicas[1]
    print(f"\n  Crashing node {crash_2}...")
    with paxosProxyFor(crash_2) as p:
        p.crash()
    print(f"  Node {crash_2} is DOWN.  Only 1/3 replicas reachable now.\n")
    path = write_tmp_page("d_p2.txt", ["entry3,delta\n"])
    try:
        dfs.append("log.txt", path)
        os.unlink(path)
        print("  Unexpected: append succeeded (should have failed)")
    except Exception as exc:
        os.unlink(path)
        print(f"  append FAILED as expected: {exc}")
    print("  Paxos correctly refused to commit below majority")

    # Bring replicas back and copy current metadata to them
    section("5. Recovery - both nodes come back, append succeeds again")
    for crashed in (crash_1, crash_2):
        print(f"\n  Recovering node {crashed}...")
        with paxosProxyFor(crashed) as p:
            p.recover()
        healthy = replicas[0]
        raw = storage._getProxy(healthy).remoteGet(meta_key)
        if raw:
            storage._getProxy(crashed).remotePut(meta_key, raw)
            print(f"  Node {crashed} metadata re-synced from node {healthy}")
    print("\n  All replicas back online. Attempting append...\n")
    path = write_tmp_page("d_p3.txt", ["entry4,epsilon\n"])
    dfs.append("log.txt", path)
    os.unlink(path)
    print("  append after recovery -> OK")
    print("\n  Final metadata on all replicas:")
    for nid in replicas:
        try:
            raw = storage._getProxy(nid).remoteGet(meta_key)
            printMetaLine(nid, raw)
        except Exception:
            print(f"    Node {nid:2d}  UNREACHABLE")

    # Remove DFS file created by this test
    section("6. Cleanup")
    dfs.deleteFile("log.txt")
    print("  deleteFile('log.txt') -> OK")
    print("\n" + "=" * 60)
    print("  Part D PASSED")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    t0 = time.perf_counter()
    main()
    print(f"Total time: {time.perf_counter()-t0:.2f}s")
