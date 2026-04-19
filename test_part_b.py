import os
import tempfile
import threading
import time

import Pyro5.api as pyro

from Chord import ChordNode, NodeInfo, node_id_for
from chordStorage import ChordStorage
from dfs import (
    DFSClient,
    parse_kv_line,
    sortable_key,
    validate_sorted_kv_text,
)
from storageNode import StorageNode

M = 4  # must match HASH_BITS


# ----------------------------
# Start a Chord peer
# ----------------------------
def start_peer(host, port, ns, bootstrap=False, join_node_id=None):
    nid = node_id_for(host, port, M)
    info = NodeInfo(nid, host, port)

    chord_node = ChordNode(info, M)
    storage_node = StorageNode(nid)

    daemon = pyro.Daemon(host=host, port=port)
    chord_uri = daemon.register(chord_node)
    storage_uri = daemon.register(storage_node)

    ns.register(f"chord.node.{nid}", chord_uri)
    ns.register(f"chord.storage.{nid}", storage_uri)

    threading.Thread(target=daemon.requestLoop, daemon=True).start()
    time.sleep(1)

    if bootstrap:
        chord_node.create()
        print(f"Node {nid} created ring")
    else:
        start = time.perf_counter()
        chord_node.join({"node_id": join_node_id, "host": host, "port": port})
        print(f"Node {nid} joined ring (Time: {time.perf_counter() - start:.4f}s)")

    return nid


# ----------------------------
# Expected sorted output
# ----------------------------
def expected_sorted_lines(text):
    pairs = []
    for line in text.splitlines():
        p = parse_kv_line(line)
        if p:
            pairs.append(p)

    pairs.sort(key=lambda kv: (sortable_key(kv[0]), kv[0], kv[1]))
    return [f"{k},{v}" for k, v in pairs]


# ----------------------------
# Main integration test
# ----------------------------
def main():
    print("\n=== Part B ===")

    ns = pyro.locate_ns()
    host = "127.0.0.1"
    base_port = 9000

    # --- Start ring ---
    node_ids = []
    node_ids.append(start_peer(host, base_port, ns, bootstrap=True))

    for i in range(1, 5):
        node_ids.append(start_peer(host, base_port + i, ns, join_node_id=node_ids[0]))

    print("\nRing nodes:", node_ids)
    print("Waiting for stabilization...")
    time.sleep(10)

    # --- DFS setup ---
    dfs = DFSClient(ChordStorage(node_ids[0]))

    in_name = "sort_input.txt"
    out_name = "sort_output.txt"

    # --- Create input file ---
    print("\n=== Creating input file ===")
    start = time.perf_counter()
    dfs.touch(in_name)
    print(f"touch {in_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")

    page_lines = [
        "50,last\n10,mid\n",
        "3,early\n30,late\n",
        "7,seven\n2,two\n",
    ]

    print("\n=== Appending pages ===")
    start = time.perf_counter()
    for i, pl in enumerate(page_lines):
        path = os.path.join(tempfile.gettempdir(), f"page_{i}.txt")
        with open(path, "w") as f:
            f.write(pl)

        dfs.append(in_name, path)
        os.unlink(path)
    print(f"append {in_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")

    # --- Verify input ---
    print("\n=== Verifying input ===")
    start = time.perf_counter()
    whole = dfs.read(in_name)
    scanned = "".join(content for _, content in dfs.iter_file_pages(in_name))
    assert whole == scanned, "Mismatch between read() and page scan"
    print(f"read {in_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")

    # --- Sort ---
    print("\n=== Running distributed sort ===")
    start = time.perf_counter()
    dfs.sort_file(in_name, out_name)
    print(f"sort_file {in_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")

    # --- Read output ---
    print("\n=== Reading sorted output ===")
    start = time.perf_counter()
    result = dfs.read(out_name)
    print(f"read {out_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")
    print("\n=== OUTPUT ===")
    print(result)

    # --- Validate ---
    print("\n=== Validating output ===")
    validate_sorted_kv_text(result)

    expected = expected_sorted_lines(whole)
    actual = [
        ln for ln in result.splitlines()
        if parse_kv_line(ln) is not None
    ]

    assert actual == expected, f"\nExpected: {expected}\nGot: {actual}"

    print("\nSUCCESS: Output is globally sorted and correct")

    # --- Cleanup ---
    print("\n=== Cleaning up ===")
    start = time.perf_counter()
    dfs.deleteFile(in_name)
    print(f"deleteFile {in_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")
    start = time.perf_counter()
    dfs.deleteFile(out_name)
    print(f"deleteFile {out_name} -> OK (Time: {time.perf_counter() - start:.4f}s)")
    print("Cleaned up DFS test files.")


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print(f"main -> OK (Time: {end - start:.4f}s)")