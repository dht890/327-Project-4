import threading
import Pyro5.api as pyro
from Pyro5.server import expose

# Sovannmonyrotn Kun 033159813

def paxosProxyFor(nodeId):
    return pyro.Proxy(f"PYRONAME:chord.paxos.{nodeId}")


@expose
class PaxosAcceptor:
    def __init__(self, nodeId):
        self.nodeId = nodeId
        self.lock = threading.RLock()
        self.crashed = False
        self.paxosState = {} # State is tracked separately for each metadata key
        self.store = {} # Committed metadata values

    def keyState(self, key): # This function will return and create Paxos state for a key if it is missing
        if key not in self.paxosState:
            self.paxosState[key] = {"ballot": -1, "acc_ballot": -1, "acc_operation": None}
        return self.paxosState[key]

    def alive(self):
        if self.crashed:
            raise RuntimeError(f"Node {self.nodeId} is down")

    def prepare(self, key, ballot): # This function will handle Phase 1 and promise not to accept lower ballots
        self.alive()
        with self.lock:
            state = self.keyState(key)
            if ballot > state["ballot"]:
                state["ballot"] = ballot
                print(f"OK   node {self.nodeId}: promise key={key!r} round={ballot} prior={state['acc_ballot']}")
                return {
                    "ok": True,
                    "acc_ballot": state["acc_ballot"],
                    "acc_operation": state["acc_operation"],
                }
            print(f"FAIL node {self.nodeId}: prepare key={key!r} round={ballot} min={state['ballot']}")
            return {"ok": False, "min_ballot": state["ballot"]}

    def accept(self, key, ballot, operation): # This function will handle Phase 2 and record the proposal if ballot is valid
        self.alive()
        with self.lock:
            state = self.keyState(key)
            if ballot >= state["ballot"]:
                state["ballot"] = ballot
                state["acc_ballot"] = ballot
                state["acc_operation"] = operation
                print(f"OK   node {self.nodeId}: accept key={key!r} round={ballot} op={operation['type']!r}")
                return {"ok": True, "ballot": ballot}
            print(f"FAIL node {self.nodeId}: accept key={key!r} round={ballot} min={state['ballot']}")
            return {"ok": False}

    def commit(self, key, ballot, operation): # This function will apply the operation to committed store after majority LEARN
        self.alive()
        with self.lock:
            operationType = operation.get("type")
            if operationType == "put":
                self.store[operation["key"]] = operation["value"]
            elif operationType == "delete":
                self.store.pop(operation["key"], None)
            else:
                return {"ok": False, "err": f"unknown operation type {operationType!r}"}
            state = self.keyState(key)
            state["acc_ballot"] = -1
            state["acc_operation"] = None
            print(f"OK   node {self.nodeId}: commit key={key!r} round={ballot} op={operationType!r}")
            return {"ok": True}

    def getCommitted(self, key):
        with self.lock:
            return self.store.get(key)

    def getNodeId(self):
        return self.nodeId

    def getPaxosLog(self):
        with self.lock:
            return dict(self.store)

    def crash(self):
        self.crashed = True
        print(f"DOWN node {self.nodeId}")

    def recover(self):
        self.crashed = False
        print(f"UP   node {self.nodeId}")
