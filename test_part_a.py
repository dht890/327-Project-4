import time
import threading
import subprocess
import Pyro5.api as pyro
from Chord import ChordNode, NodeInfo, node_id_for
from storageNode import StorageNode
from chordStorage import ChordStorage
from dfs import DFSClient

M = 8  # must match HASH_BITS in dfs.py

def startPeer(host, port, ns, bootstrap=False, joinNodeId=None):
    """Start a ChordNode + StorageNode in a Pyro daemon on a background thread."""
    nid = node_id_for(host, port, M)
    info = NodeInfo(nid, host, port)

    # Create Chord node
    chordNode = ChordNode(info, M)

    # Create Storage node
    storageNode = StorageNode(nid)

    # Register both in a single daemon
    daemon = pyro.Daemon(host=host, port=port)
    chordUri = daemon.register(chordNode)
    storageUri = daemon.register(storageNode)

    ns.register(f"chord.node.{nid}", chordUri)
    ns.register(f"chord.storage.{nid}", storageUri)

    if bootstrap:
        chordNode.create()
        print(f"Node {nid} created new ring")
    else:
        chordNode.join({"node_id": joinNodeId, "host": host, "port": port})
        print(f"Node {nid} joined ring")

    # Run daemon in background
    threading.Thread(target=daemon.requestLoop, daemon=True).start()

    # Start Chord maintenance
    chordNode.start_maintenance()

    return nid

def main():
    # Connect to Pyro name server (must be running already)
    ns = pyro.locate_ns()

    host = "127.0.0.1"
    basePort = 9000

    # --- Start 5 peers ---
    print("=== Starting 5 Chord peers ===")
    nodeIds = []

    # First node bootstraps the ring
    nid = startPeer(host, basePort, ns, bootstrap=True)
    nodeIds.append(nid)
    time.sleep(1)

    # Remaining 4 join via the first node
    for i in range(1, 5):
        nid = startPeer(host, basePort + i, ns, joinNodeId=nodeIds[0])
        nodeIds.append(nid)
        time.sleep(1)

    print(f"\nNode IDs: {nodeIds}")
    print("Waiting for ring to stabilize...")
    time.sleep(10)

    # --- Create DFS client ---
    storage = ChordStorage(nodeIds[0])
    dfs = DFSClient(storage)

    # --- Test touch ---
    print("\n=== Testing touch ===")
    dfs.touch("test.txt")
    print(f"touch test.txt -> OK")

    # --- Test ls ---
    print("\n=== Testing ls ===")
    print(f"ls -> {dfs.ls()}")

    # --- Create local test files in current directory ---
    for i in range(3):
        with open(f"page{i}.txt", "w") as f:
            f.write(f"This is page {i}\nLine 2 of page {i}\nLine 3 of page {i}\n")

    # --- Test append ---
    print("\n=== Testing append (3 pages) ===")
    for i in range(3):
        dfs.append("test.txt", f"page{i}.txt")
        print(f"append page{i}.txt -> OK")

    # --- Test stat ---
    print("\n=== Testing stat ===")
    info = dfs.stat("test.txt")
    print(f"stat -> {info}")

    # --- Test read ---
    print("\n=== Testing read ===")
    content = dfs.read("test.txt")
    print(f"read ->\n{content}")

    # --- Test head ---
    print("\n=== Testing head(2) ===")
    print(dfs.head("test.txt", 2))

    # --- Test tail ---
    print("\n=== Testing tail(2) ===")
    print(dfs.tail("test.txt", 2))

    # --- Test delete ---
    print("\n=== Testing delete ===")
    dfs.deleteFile("test.txt")
    print(f"delete test.txt -> OK")
    print(f"ls after delete -> {dfs.ls()}")

    print("\n=== All Part A tests passed ===")

if __name__ == "__main__":
    main()