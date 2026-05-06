import json
import time
from concurrent.futures import ThreadPoolExecutor, wait

from chordStorage import ChordStorage
from paxos import paxosProxyFor

REPLICATION_FACTOR = 3

# Sovannmonyrotn Kun 033159813

class PaxosLeader:
    def __init__(
        self,
        leaderId,
        acceptorIds,
        maxRetries=3,
        initial_ballot_seq=0,
        paxos_proxy_getter=None,
    ):
        self.leaderId = leaderId
        self.acceptorIds = acceptorIds # Nodes that store this metadata key
        self.maxRetries = maxRetries
        # Sequence must persist across PaxosLeader instances so ballots stay above acceptors' promised ballot
        self.ballotSeq = initial_ballot_seq
        self._pax_proxy = paxos_proxy_getter

    def nextBallot(self): # This function will generate a unique ballot number using leader ID as tiebreaker
        self.ballotSeq += 1
        return self.ballotSeq * 10000 + self.leaderId # Leader ID as low-order tiebreaker

    def propose(self, metaKey, operation): # This function will run full Paxos round and return True if majority committed
        majority = len(self.acceptorIds) // 2 + 1 # Need more than half to agree
        n = len(self.acceptorIds)

        def px(node_id):
            return self._pax_proxy(node_id)

        for attempt in range(1, self.maxRetries + 1):
            ballot = self.nextBallot()
            print()
            print(f"Leader {self.leaderId}: round={ballot} key={metaKey!r} attempt={attempt}/{self.maxRetries}")

            # Phase 1 — prepare in parallel
            promises = []
            highestPrior = {"ballot": -1, "operation": None}

            def call_prepare(node_id):
                return node_id, px(node_id).prepare(metaKey, ballot)

            with ThreadPoolExecutor(max_workers=max(n, 1)) as pool:
                futs = [pool.submit(call_prepare, nid) for nid in self.acceptorIds]
                wait(futs)
                for fut in futs:
                    try:
                        _node_id, resp = fut.result()
                        if resp["ok"]:
                            promises.append(resp)
                            if resp["acc_ballot"] > highestPrior["ballot"]:
                                highestPrior = {
                                    "ballot": resp["acc_ballot"],
                                    "operation": resp["acc_operation"],
                                }
                    except Exception as exc:
                        print(f"WARN leader {self.leaderId}: prepare failed: {exc}")

            if len(promises) < majority:
                print(f"FAIL leader {self.leaderId}: phase 1 promises={len(promises)}/{len(self.acceptorIds)}")
                time.sleep(0.15 * attempt)
                continue

            print(f"OK   leader {self.leaderId}: phase 1 promises={len(promises)}/{len(self.acceptorIds)}")

            if highestPrior["operation"] is not None:
                print(f"INFO leader {self.leaderId}: adopting prior ballot {highestPrior['ballot']}")
                operation = highestPrior["operation"]

            # Phase 2 — accept in parallel
            learned = []

            def call_accept(node_id):
                return node_id, px(node_id).accept(metaKey, ballot, operation)

            with ThreadPoolExecutor(max_workers=max(n, 1)) as pool:
                futs = [pool.submit(call_accept, nid) for nid in self.acceptorIds]
                wait(futs)
                for fut in futs:
                    try:
                        node_id, resp = fut.result()
                        if resp["ok"]:
                            learned.append(node_id)
                    except Exception as exc:
                        print(f"WARN leader {self.leaderId}: accept failed: {exc}")

            if len(learned) < majority:
                print(f"FAIL leader {self.leaderId}: phase 2 accepts={len(learned)}/{len(self.acceptorIds)}")
                time.sleep(0.15 * attempt)
                continue

            print(f"OK   leader {self.leaderId}: phase 2 accepts={len(learned)}/{len(self.acceptorIds)}")

            def call_commit(node_id):
                px(node_id).commit(metaKey, ballot, operation)
                return node_id

            with ThreadPoolExecutor(max_workers=max(n, 1)) as pool:
                futs = [pool.submit(call_commit, nid) for nid in self.acceptorIds]
                wait(futs)
                for fut in futs:
                    try:
                        fut.result()
                    except Exception as exc:
                        print(f"WARN leader {self.leaderId}: commit failed: {exc}")

            print(f"OK   leader {self.leaderId}: consensus key={metaKey!r} round={ballot}")
            return True

        print(f"FAIL leader {self.leaderId}: no consensus after {self.maxRetries} attempts")
        return False


class ReplicatedChordStorage(ChordStorage):
    def __init__(self, nodeId):
        super().__init__(nodeId)
        # Per (leader, metadata key): last Paxos ballot sequence used locally (acceptors retain higher promised ballots)
        self._paxosBallotSeq = {}
        self._paxos_proxy_cache = {}

    def _getPaxosProxy(self, node_id: int):
        if node_id not in self._paxos_proxy_cache:
            self._paxos_proxy_cache[node_id] = paxosProxyFor(node_id)
        return self._paxos_proxy_cache[node_id]

    def getReplicas(self, key): # This function will return up to REPLICATION_FACTOR node IDs using successor placement
        primary = self._findResponsible(key)
        primaryId = primary["node_id"]
        replicas = [primaryId]
        cur = primaryId

        for _ in range(REPLICATION_FACTOR - 1):
            try:
                succ = self._getChordProxy(cur).get_successor()
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
        seq_key = (leaderId, metaKey)
        leader = PaxosLeader(
            leaderId,
            replicas,
            initial_ballot_seq=self._paxosBallotSeq.get(seq_key, 0),
            paxos_proxy_getter=self._getPaxosProxy,
        )
        ok = leader.propose(metaKey, operation)
        self._paxosBallotSeq[seq_key] = leader.ballotSeq
        if ok:
            self.syncStorage(metaKey, replicas, value) # Sync to storage so existing read paths still work
        return ok

    def paxosDelete(self, metaKey): # This function will run Paxos to commit a delete to all replicas
        replicas = self.getReplicas(metaKey)
        leaderId = self.leaderFor(replicas)
        print(f"Replication DEL key={metaKey!r} replicas={replicas} leader={leaderId}")
        operation = {"type": "delete", "key": metaKey}
        seq_key = (leaderId, metaKey)
        leader = PaxosLeader(
            leaderId,
            replicas,
            initial_ballot_seq=self._paxosBallotSeq.get(seq_key, 0),
            paxos_proxy_getter=self._getPaxosProxy,
        )
        ok = leader.propose(metaKey, operation)
        self._paxosBallotSeq[seq_key] = leader.ballotSeq
        if ok:
            for nodeId in replicas:
                try:
                    self._getProxy(nodeId).remoteDelete(metaKey) # Remove from each StorageNode too
                except Exception as e:
                    print(f"WARN replication: storage delete failed on node {nodeId}: {e}")
        return ok

    def syncStorage(self, metaKey, replicas, value): # This function will push committed metadata value to every replica StorageNode
        n = len(replicas)

        def put_one(node_id):
            self._getProxy(node_id).remotePut(metaKey, value)

        with ThreadPoolExecutor(max_workers=max(n, 1)) as pool:
            futs = [pool.submit(put_one, nid) for nid in replicas]
            wait(futs)
            for fut in futs:
                try:
                    fut.result()
                except Exception as e:
                    print(f"WARN replication: storage put failed: {e}")

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
