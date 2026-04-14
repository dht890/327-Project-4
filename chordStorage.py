import Pyro5.api as pyro
from Chord import proxy_for

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

    def _findResponsible(self, key):
        """Use Chord routing to find which node is responsible for this key."""
        with proxy_for(self.nodeId) as p:
            return p.find_successor(key)

    def put(self, key, value):
        responsible = self._findResponsible(key)
        with storageProxyFor(responsible["node_id"]) as p:
            return p.remotePut(key, value)

    def get(self, key):
        responsible = self._findResponsible(key)
        with storageProxyFor(responsible["node_id"]) as p:
            return p.remoteGet(key)

    def delete(self, key):
        responsible = self._findResponsible(key)
        with storageProxyFor(responsible["node_id"]) as p:
            return p.remoteDelete(key)