import Pyro5.api as pyro
from Pyro5.server import expose

@expose
class StorageNode:
    """Runs alongside each ChordNode to handle key-value storage."""

    def __init__(self, nodeId):
        self.nodeId = nodeId
        self.localStore = {}

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
