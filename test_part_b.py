#test_part_b.py
import os
import tempfile
import threading
import time
import unittest

import Pyro5.api as pyro

from Chord import ChordNode, NodeInfo, node_id_for
from chordStorage import ChordStorage
from dfs import (
    DFSClient,
    decode_page_blob,
    parse_kv_line,
    sortable_key,
    validate_sorted_kv_text,
)
from storageNode import StorageNode

M = 4  # must match HASH_BITS in dfs.py and Chord m


def start_peer(host: str, port: int, ns, bootstrap: bool = False, join_node_id=None):
    nid = node_id_for(host, port, M)
    info = NodeInfo(nid, host, port)

    chord_node = ChordNode(info, M)
    storage_node = StorageNode(nid)

    daemon = pyro.Daemon(host=host, port=port)

    chord_uri = daemon.register(chord_node)
    storage_uri = daemon.register(storage_node)

    ns.register(f"chord.node.{nid}", chord_uri)
    ns.register(f"chord.storage.{nid}", storage_uri)

    # IMPORTANT: start daemon BEFORE heavy Chord activity
    t = threading.Thread(target=daemon.requestLoop, daemon=True)
    t.start()

    time.sleep(0.3)  # 🔥 allows Pyro to fully bind object table

    if bootstrap:
        chord_node.create()
        print(f"Node {nid} created new ring")
    else:
        if join_node_id is None:
            raise ValueError("join_node_id required when not bootstrap")
        chord_node.join({"node_id": join_node_id, "host": host, "port": port})
        print(f"Node {nid} joined ring")

    chord_node.start_maintenance(stabilize_period=0.5, fix_fingers_period=0.5)

    return nid


class TestDecodePageBlob(unittest.TestCase):
    def test_json_content(self):
        import json

        self.assertEqual(
            decode_page_blob(json.dumps({"pageNo": 0, "content": "a,b\n"})),
            "a,b\n",
        )

    def test_legacy_string(self):
        self.assertEqual(decode_page_blob("plain"), "plain")


class TestParseAndValidate(unittest.TestCase):
    def test_parse_kv_basic(self):
        self.assertEqual(parse_kv_line("a,b"), ("a", "b"))
        self.assertEqual(parse_kv_line("  10 ,  hello  "), ("10", "hello"))

    def test_parse_kv_invalid(self):
        self.assertIsNone(parse_kv_line(""))
        self.assertIsNone(parse_kv_line("nocomma"))
        self.assertIsNone(parse_kv_line("   "))

    def test_validate_sorted_ok(self):
        validate_sorted_kv_text("1,x\n2,y\n3,z\n")
        validate_sorted_kv_text("apple,1\nbanana,2\n")

    def test_validate_sorted_rejects(self):
        with self.assertRaises(ValueError):
            validate_sorted_kv_text("2,a\n1,b\n")

    def test_validate_ignores_blank_lines(self):
        validate_sorted_kv_text("\n1,a\n\n2,b\n")


class TestAssembleSortedOutput(unittest.TestCase):
    def test_empty_shards(self):
        body, lines = DFSClient._assemble_sorted_output([])
        self.assertEqual(body, "")
        self.assertEqual(lines, [])

    def test_k_way_merge(self):
        chunks = [
            [("3", "c"), ("5", "e")],
            [("1", "a"), ("4", "d")],
            [("2", "b")],
        ]
        body, lines = DFSClient._assemble_sorted_output(chunks)
        self.assertEqual(
            lines,
            ["1,a", "2,b", "3,c", "4,d", "5,e"],
        )
        validate_sorted_kv_text(body)


def _expected_sorted_lines(text: str):
    pairs = []
    for line in text.splitlines():
        p = parse_kv_line(line)
        if p:
            pairs.append(p)
    pairs.sort(key=lambda kv: (sortable_key(kv[0]), kv[0], kv[1]))
    return [f"{k},{v}" for k, v in pairs]


def main():
    """multi-page DFS input, distributed sort, read-back validation."""
    print("\n=== Part B  ===")
    ns = pyro.locate_ns()
    
    host = "127.0.0.1"
    base_port = 9000
    node_ids = []

    nid = start_peer(host, base_port, ns, bootstrap=True)
    node_ids.append(nid)
    time.sleep(0.5)

    for i in range(1, 5):
        start = time.perf_counter()
        nid = start_peer(host, base_port + i, ns, bootstrap=False, join_node_id=node_ids[0])
        print("Node joined in", time.perf_counter() - start, "seconds")
        node_ids.append(nid)
        time.sleep(0.5)

    print(f"\nRing node IDs: {node_ids}")
    print("Waiting for ring to stabilize...\n")
    time.sleep(5)

    # --- Create DFS client ---
    storage = ChordStorage(node_ids[0])
    dfs = DFSClient(storage)

    # --- Create input and output files ---
    in_name = "sort_input.txt"
    out_name = "sort_output.txt"

    # --- Create input file ---
    dfs.touch(in_name)

    # --- Create input pages ---
    page_lines = [
        "50,last\n10,mid\n",
        "3,early\n30,late\n",
        "7,seven\n2,two\n",
    ]
    # --- Append input pages to input file ---
    for i, pl in enumerate(page_lines):
        path = os.path.join(tempfile.gettempdir(), f"part_b_page_{i}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(pl)
        dfs.append(in_name, path)
        try:
            os.unlink(path)
        except OSError:
            pass

    # --- Check input file metadata ---
    st = dfs.stat(in_name)  
    assert st["numPages"] == 3, st

    # --- Check input file content ---
    scanned = "".join(content for _, content in dfs.iter_file_pages(in_name))
    whole = dfs.read(in_name)
    assert scanned == whole, "page scan must match read()"

    # --- Sort input file ---
    dfs.sort_file(in_name, out_name)

    # --- Check output file content ---
    result = dfs.read(out_name)
    validate_sorted_kv_text(result)

    # --- Check output file content ---
    expected = _expected_sorted_lines(whole)
    actual = [ln for ln in result.splitlines() if parse_kv_line(ln) is not None]
    assert actual == expected, f"expected {expected!r}, got {actual!r}"

    print("\nIntegration OK: page scan == read, output globally sorted and validated.")

    dfs.deleteFile(in_name)
    dfs.deleteFile(out_name)
    print("\nCleaned up DFS test files.")


if __name__ == "__main__":
    main()
