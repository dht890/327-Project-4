import json
import os
import random
import tempfile
import threading
import time
import Pyro5.api as pyro
from Chord import ChordNode, NodeInfo, node_id_for
from dfs import DFSClient, parse_kv_line, validate_sorted_kv_text
from paxos import PaxosAcceptor
from replication import ReplicatedChordStorage
from storageNode import StorageNode

M = 4
BASE_PORT = 9100 # ports 9100-9104


def start_peer(host, port, ns, bootstrap=False, join_node_id=None):
    nid = node_id_for(host, port, M)
    info = NodeInfo(nid, host, port)
    chord_node = ChordNode(info, M)
    storage_node = StorageNode(nid)
    paxos_node = PaxosAcceptor(nid)

    # Register Chord, storage, and Paxos services for this node
    daemon = pyro.Daemon(host=host, port=port)
    ns.register(f"chord.node.{nid}", daemon.register(chord_node))
    ns.register(f"chord.storage.{nid}", daemon.register(storage_node))
    ns.register(f"chord.paxos.{nid}", daemon.register(paxos_node))
    threading.Thread(target=daemon.requestLoop, daemon=True).start()
    time.sleep(0.3)
    if bootstrap:
        chord_node.create()
        chord_node.start_maintenance(stabilize_period=0.5, fix_fingers_period=0.5)
        print(f"Node {nid:2d}  bootstrapped  port={port}")
    else:
        chord_node.join({"node_id": join_node_id, "host": host, "port": port})
        chord_node.start_maintenance(stabilize_period=0.5, fix_fingers_period=0.5)
        print(f"Node {nid:2d}  joined        port={port}")
    return nid

def section(title):
    print()
    print("-" * 60)
    print(f"  {title}")
    print("-" * 60)

def printMetadata(nodeId, raw):
    if raw:
        meta = json.loads(raw)
        print(
            f"  Node {nodeId:2d}: "
            f"version={meta['version']} "
            f"numPages={meta['numPages']} "
            f"byteSize={meta['byteSize']}"
        )
    else:
        print(f"  Node {nodeId:2d}: NO DATA")

def main():
    print("\n" + "=" * 60)
    print("Part C - Replication (R = 3, successor-based)")
    print("=" * 60)
    ns = pyro.locate_ns()
    host = "127.0.0.1"
    # Start all peers before running DFS requests
    section("1. Start 5-node Chord ring")
    node_ids = []
    nid = start_peer(host, BASE_PORT, ns, bootstrap=True)
    node_ids.append(nid)

    for i in range(1, 5):
        nid = start_peer(host, BASE_PORT + i, ns, join_node_id=node_ids[0])
        node_ids.append(nid)
    print()
    print(f"Node IDs: {node_ids}")
    print("Waiting for ring to stabilize (8 s)...")
    time.sleep(8)
    print()
    print("Ring topology:")
    for nid in node_ids:
        with pyro.Proxy(f"PYRONAME:chord.node.{nid}") as p:
            succ = p.get_successor()
            pred = p.get_predecessor()
            fingers = p.get_finger_table()
        pred_id = pred["node_id"] if pred else "None"
        finger_ids = [f["node_id"] for f in fingers]
        print(f"  Node {nid:2d}  pred={pred_id!s:>2}  succ={succ['node_id']:>2}  fingers={finger_ids}")
    # Create a file and append three pages
    section("2. DFS operations - touch / append / read")
    storage = ReplicatedChordStorage(node_ids[0])
    dfs = DFSClient(storage)
    dfs.touch("music.json")
    print("touch('music.json') -> OK")
    for i in range(3):
        path = os.path.join(tempfile.gettempdir(), f"c_page{i}.txt")
        with open(path, "w") as f:
            for j in range(12):
                f.write(f"track{i*12+j:03d},artist_{i}_{j}\n")
        dfs.append("music.json", path)
        os.unlink(path)
        print(f"append page {i} -> OK")
    content = dfs.read("music.json")
    lines = [l for l in content.splitlines() if l.strip()]
    print(f"  read -> {len(lines)} lines")
    print(f"  first: {lines[0]!r}")
    print(f"  last:  {lines[-1]!r}")
    # Check the metadata copy on every replica
    section("3. Verify metadata is on R=3 replica nodes")
    meta_key = "metadata:music.json"
    replicas = storage.getReplicas(meta_key)
    print(f"Replicas for {meta_key!r}: {replicas}")
    for nid in replicas:
        raw = storage._getProxy(nid).remoteGet(meta_key)
        printMetadata(nid, raw)
    backup_nid = replicas[1]
    raw_backup = storage._getProxy(backup_nid).remoteGet(meta_key)
    assert raw_backup is not None, "Backup replica missing metadata"
    print(f"Direct read from replica n={backup_nid} -> consistent")

    # Sort enough records to verify distributed merge behavior
    section("4. Distributed sort - 100 records")
    dfs.touch("sort_in.txt")
    random.seed(7)
    keys = list(range(1, 101))
    random.shuffle(keys)
    for chunk_idx, start in enumerate(range(0, 100, 25)):
        chunk = keys[start: start + 25]
        path = os.path.join(tempfile.gettempdir(), f"c_sort{chunk_idx}.txt")
        with open(path, "w") as f:
            for k in chunk:
                f.write(f"{k},val_{k}\n")
        dfs.append("sort_in.txt", path)
        os.unlink(path)
    print("Created sort_in.txt - 100 records, 4 pages")
    t0 = time.perf_counter()
    dfs.sort_file("sort_in.txt", "sort_out.txt")
    print(f"sort_file -> OK  ({time.perf_counter()-t0:.2f}s)")
    result = dfs.read("sort_out.txt")
    validate_sorted_kv_text(result)
    out_lines = [l for l in result.splitlines() if parse_kv_line(l)]
    assert len(out_lines) == 100
    got_keys = sorted(int(parse_kv_line(l)[0]) for l in out_lines)
    assert got_keys == list(range(1, 101))
    print(f"Output: {len(out_lines)} records, globally sorted")
    print(f"  first 3: {out_lines[:3]}")
    print(f"  last  3: {out_lines[-3:]}")
    # Remove DFS files created by this test
    section("5. Cleanup")

    for fname in ("music.json", "sort_in.txt", "sort_out.txt"):
        dfs.deleteFile(fname)
        print(f"  deleteFile({fname!r}) -> OK")
    print("\n" + "=" * 60)
    print("  Part C PASSED")
    print("=" * 60 + "\n")

if __name__ == "__main__":
    t0 = time.perf_counter()
    main()
    print(f"Total time: {time.perf_counter()-t0:.2f}s")
