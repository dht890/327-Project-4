import json
import time

from Chord import proxy_for
from chordStorage import ChordStorage
from paxos import paxosProxyFor

REPLICATION_FACTOR = 3

# Sovannmonyrotn Kun 033159813

class PaxosLeader:
    def __init__(self, leaderId, acceptorIds, maxRetries=3):
        self.leaderId = leaderId
        self.acceptorIds = acceptorIds # Nodes that store this metadata key
        self.maxRetries = maxRetries
        self.ballotSeq = 0 # Incremented each round to keep ballots strictly increasing

    def nextBallot(self): # This function will generate a unique ballot number using leader ID as tiebreaker
        self.ballotSeq += 1
        return self.ballotSeq * 10000 + self.leaderId # Leader ID as low-order tiebreaker

    def propose(self, metaKey, operation): # This function will run full Paxos round and return True if majority committed
        majority = len(self.acceptorIds) // 2 + 1 # Need more than half to agree

        for attempt in range(1, self.maxRetries + 1):
            ballot = self.nextBallot()
            print()
            print(f"Leader {self.leaderId}: round={ballot} key={metaKey!r} attempt={attempt}/{self.maxRetries}")

            # Phase 1 will collect promises from acceptors
            promises = []
            highestPrior = {"ballot": -1, "operation": None} # Tracks most recently accepted operation seen

            for nodeId in self.acceptorIds:
                try:
                    with paxosProxyFor(nodeId) as p:
                        resp = p.prepare(metaKey, ballot)
                    if resp["ok"]:
                        promises.append(resp)
                        if resp["acc_ballot"] > highestPrior["ballot"]: # Keep track of highest accepted ballot
                            highestPrior = {"ballot": resp["acc_ballot"], "operation": resp["acc_operation"]}
                except Exception as exc:
                    print(f"WARN leader {self.leaderId}: prepare failed on node {nodeId}: {exc}")

            if len(promises) < majority:
                print(f"FAIL leader {self.leaderId}: phase 1 promises={len(promises)}/{len(self.acceptorIds)}")
                time.sleep(0.15 * attempt)
                continue

            print(f"OK   leader {self.leaderId}: phase 1 promises={len(promises)}/{len(self.acceptorIds)}")

            # If an acceptor already had accepted operation, adopt it for Paxos safety
            if highestPrior["operation"] is not None:
                print(f"INFO leader {self.leaderId}: adopting prior ballot {highestPrior['ballot']}")
                operation = highestPrior["operation"]

            # Phase 2 will send operation to all acceptors
            learned = []

            for nodeId in self.acceptorIds:
                try:
                    with paxosProxyFor(nodeId) as p:
                        resp = p.accept(metaKey, ballot, operation)
                    if resp["ok"]:
                        learned.append(nodeId)
                except Exception as exc:
                    print(f"WARN leader {self.leaderId}: accept failed on node {nodeId}: {exc}")

            if len(learned) < majority:
                print(f"FAIL leader {self.leaderId}: phase 2 accepts={len(learned)}/{len(self.acceptorIds)}")
                time.sleep(0.15 * attempt)
                continue

            print(f"OK   leader {self.leaderId}: phase 2 accepts={len(learned)}/{len(self.acceptorIds)}")

            # This will tell all replicas to apply operation, even ones that missed Phase 2
            for nodeId in self.acceptorIds:
                try:
                    with paxosProxyFor(nodeId) as p:
                        p.commit(metaKey, ballot, operation)
                except Exception as exc:
                    print(f"WARN leader {self.leaderId}: commit failed on node {nodeId}: {exc}")

            print(f"OK   leader {self.leaderId}: consensus key={metaKey!r} round={ballot}")
            return True

        print(f"FAIL leader {self.leaderId}: no consensus after {self.maxRetries} attempts")
        return False


class ReplicatedChordStorage(ChordStorage):
    def getReplicas(self, key): # This function will return up to REPLICATION_FACTOR node IDs using successor placement
        primary = self._findResponsible(key)
        primaryId = primary["node_id"]
        replicas = [primaryId]
        cur = primaryId

        for _ in range(REPLICATION_FACTOR - 1):
            try:
                with proxy_for(cur) as p:
                    succ = p.get_successor()
                nxt = succ["node_id"]
            except Exception:
                break
            if nxt == primaryId or nxt in replicas: # Stop if ring is too small or wrapped around
                break
            replicas.append(nxt)
            cur = nxt

        return replicas

    def leaderFor(self, replicas): # This function will return the lowest node ID as deterministic leader
        return min(replicas)

    def paxosPut(self, metaKey, value): # This function will run Paxos to commit a put to all replicas
        replicas = self.getReplicas(metaKey)
        leaderId = self.leaderFor(replicas)
        print(f"Replication PUT key={metaKey!r} replicas={replicas} leader={leaderId}")
        operation = {"type": "put", "key": metaKey, "value": value}
        ok = PaxosLeader(leaderId, replicas).propose(metaKey, operation)
        if ok:
            self.syncStorage(metaKey, replicas, value) # Sync to storage so existing read paths still work
        return ok

    def paxosDelete(self, metaKey): # This function will run Paxos to commit a delete to all replicas
        replicas = self.getReplicas(metaKey)
        leaderId = self.leaderFor(replicas)
        print(f"Replication DEL key={metaKey!r} replicas={replicas} leader={leaderId}")
        operation = {"type": "delete", "key": metaKey}
        ok = PaxosLeader(leaderId, replicas).propose(metaKey, operation)
        if ok:
            for nodeId in replicas:
                try:
                    self._getProxy(nodeId).remoteDelete(metaKey) # Remove from each StorageNode too
                except Exception as e:
                    print(f"WARN replication: storage delete failed on node {nodeId}: {e}")
        return ok

    def syncStorage(self, metaKey, replicas, value): # This function will push committed metadata value to every replica StorageNode
        for nodeId in replicas:
            try:
                self._getProxy(nodeId).remotePut(metaKey, value)
            except Exception as e:
                print(f"WARN replication: storage put failed on node {nodeId}: {e}")

    def put(self, key, value):
        if key.startswith("metadata:"):
            return self.paxosPut(key, str(value))
        return super().put(key, value) # Non-metadata keys go straight to base ChordStorage

    def delete(self, key):
        if key.startswith("metadata:"):
            return self.paxosDelete(key)
        return super().delete(key) # Non-metadata keys go straight to base ChordStorage

    def get(self, key): # This function will try primary first, then fall back so reads survive a crash
        if key.startswith("metadata:"):
            replicas = self.getReplicas(key)
            for nodeId in replicas:
                try:
                    val = self._getProxy(nodeId).remoteGet(key)
                    if val is not None:
                        return val
                except Exception:
                    pass # Node is down, try the next replica
            return None
        return super().get(key)

    def append(self, metaKey, pageKey, content): # This function will store page then update metadata across all replicas using Paxos
        raw = self.get(metaKey)
        if raw is None:
            raise FileNotFoundError(f"Metadata not found: {metaKey}")

        meta = json.loads(raw)
        pageNo = meta["numPages"] # Current page count becomes index for new page

        pageNode = self._findResponsible(pageKey)
        self._getProxy(pageNode["node_id"]).remotePut(
            pageKey,
            json.dumps({"pageNo": pageNo, "content": content}),
        )

        meta["pages"].append({"pageNo": pageNo, "key": pageKey})
        meta["numPages"] += 1
        meta["byteSize"] += len(content.encode())
        meta["version"] += 1

        ok = self.paxosPut(metaKey, json.dumps(meta)) # Commit updated metadata to all replicas
        if not ok:
            raise RuntimeError(f"Paxos failed to commit metadata update for {metaKey}")
        return True
