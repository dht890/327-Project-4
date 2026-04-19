#ChordStorage.py
import time
import Pyro5.api as pyro
import json
from Chord import proxy_for
from dfs import dfsHash

def storageProxyFor(nodeId):
    """Get a Pyro proxy to the StorageNode registered for a given Chord node."""
    return pyro.Proxy(f"PYRONAME:chord.storage.{nodeId}")

class ChordStorage:
    """
    Bridges DFSClient and the provided Chord.
    
    DFSClient calls: put(key, value), get(key), delete(key)
    This class uses Chord's find_successor to route to the right node,
    then calls StorageNode on that node to do the actual storage.
    """

    def __init__(self, nodeId):
        """nodeId: the Chord node ID to use as entry point for routing."""
        self.nodeId = nodeId
        self.proxy_cache = {}
        
    def _getProxy(self, nodeId):
        """
        Return cached Pyro proxy instead of creating a new one every time.
        """
        if nodeId not in self.proxy_cache:
            self.proxy_cache[nodeId] = storageProxyFor(nodeId)
        return self.proxy_cache[nodeId]

    def _findResponsible(self, key):
        """Use Chord routing to find which node is responsible for this key."""
        start = time.perf_counter()
        with proxy_for(self.nodeId) as p:
            result = p.find_successor(key)
        print(f"find_successor took {time.perf_counter() - start:.4f}s")
        return result

    def put(self, key, value):
        responsible = self._findResponsible(key)
        p = self._getProxy(responsible["node_id"])
        return p.remotePut(key, value)

    def get(self, key):
        responsible = self._findResponsible(key)
        p = self._getProxy(responsible["node_id"])
        return p.remoteGet(key)

    def delete(self, key):
        responsible = self._findResponsible(key)
        p = self._getProxy(responsible["node_id"])
        return p.remoteDelete(key)

    def list_ring_nodes(self):
        """Walk the Chord ring from the entry node and return every node id once."""
        nodes = []
        start = self.nodeId
        cur = start
        for _ in range(4096):
            nodes.append(cur)
            with proxy_for(cur) as p:
                succ = p.get_successor()
            nxt = succ["node_id"]
            if nxt == start:
                break
            cur = nxt
        return nodes

    def sort_reset_all(self):
        for nid in self.list_ring_nodes():
            p = self._getProxy(nid)
            p.remoteSortClear()

    def route_sort_record(self, sort_key: str, value: str):
        route_key = dfsHash(sort_key)
        responsible = self._findResponsible(route_key)
        p = self._getProxy(responsible["node_id"])
        return p.remoteSortAppend(sort_key, value)

    def sort_collect_sorted_shards(self):
        """Return non-empty locally sorted shards from every ring node (clears each buffer)."""
        chunks = []
        for nid in self.list_ring_nodes():
            batch = self._getProxy(nid).remoteSortGetSorted()
            if batch:
                chunks.append(batch)
        return chunks

    def append(self, metaKey, pageKey, content):
        responsible = self._findResponsible(metaKey)
        p = self._getProxy(responsible["node_id"])

        # --- 1. Get metadata ---
        raw = p.remoteGet(metaKey)
        if raw is None:
            raise FileNotFoundError("Metadata not found")

        meta = json.loads(raw)

        # --- 2. Compute page ---
        pageNo = meta["numPages"]

        pageData = {
            "pageNo": pageNo,
            "content": content
        }

        # --- 3. Store page ---
        p.remotePut(pageKey, json.dumps(pageData))

        # --- 4. Update metadata ---
        meta["pages"].append({"pageNo": pageNo, "key": pageKey})
        meta["numPages"] += 1
        meta["byteSize"] += len(content.encode())
        meta["version"] += 1

        # --- 5. Store updated metadata ---
        p.remotePut(metaKey, json.dumps(meta))

        return True