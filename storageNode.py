import Pyro5.api as pyro
from Pyro5.server import expose

from dfs import sortable_key


@expose
class StorageNode:
    """Runs alongside each ChordNode to handle key-value storage."""

    def __init__(self, nodeId):
        self.nodeId = nodeId
        self.localStore = {}
        self._sort_batch = []

    def remotePut(self, key, value):
        self.localStore[key] = value
        return True

    def remoteGet(self, key):
        return self.localStore.get(key, None)

    def remoteDelete(self, key):
        if key in self.localStore:
            del self.localStore[key]
            return True
        return False

    def getKeys(self):
        return list(self.localStore.keys())

    def remoteSortClear(self):
        self._sort_batch = []
        return True

    def remoteSortAppend(self, sort_key: str, value: str):
        self._sort_batch.append((sort_key, value))
        return True

    def remoteSortGetSorted(self):
        self._sort_batch.sort(key=lambda kv: sortable_key(kv[0]))
        out = list(self._sort_batch)
        self._sort_batch = []
        return out
