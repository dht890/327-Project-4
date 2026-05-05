# CECS 327 Project 4: Distributed File System on Chord DHT

For this project, we are implementing a distributed file system using a Chord DHT.
The system stores files as metadata and pages. Chord is used to route keys to the
correct node, and replication plus Paxos are used to keep file metadata consistent.

## System model and assumptions
- Nodes communicate using Pyro5 remote calls.
- Each Chord node has a storage service and a Paxos acceptor.
- File pages are stored on the node responsible for the page key.
- File metadata is replicated on 3 successor nodes.
- Paxos is used for metadata updates so replicas agree before applying changes.
- Crash-stop failures are simulated for Part D.

## Requirements
- Python 3
- Pyro5

Install Pyro5:
```bash
pip install Pyro5
```

## Files
- Chord.py
- storageNode.py
- chordStorage.py
- dfs.py
- paxos.py
- replication.py
- test_part_a.py
- test_part_b.py
- test_part_c.py
- test_part_d.py

## Diagram
```
+----------------------+
| DFS Client CLI       |
+----------------------+
          |
          v
+----------------------+
| DFS API              |
| touch/append/read    |
| sort_file/delete     |
+----------------------+
          |
          v
+----------------------+
| Replication Layer    |
| Paxos leader/follower|
+----------------------+
          |
          v
+----------------------+
| Chord Routing Layer  |
| locateSuccessor()    |
| put/get/delete       |
+----------------------+
          |
          v
+----------------------+
| Local Storage        |
| metadata/pages/log   |
+----------------------+
```

## How to Run

Start the Pyro5 name server first:
```bash
python3 -m Pyro5.nameserver
```

Then open another terminal and run the part you want.

### For Part A
Basic DFS operations:
```bash
python3 test_part_a.py
```

### For Part B
Distributed sort:
```bash
python3 test_part_b.py
```

### For Part C
Replication with 3 metadata replicas:
```bash
python3 test_part_c.py
```

### For Part D
Paxos with crash and recovery:
```bash
python3 test_part_d.py
```

Create output logs:
```bash
python3 test_part_c.py > part_c_output.txt
python3 test_part_d.py > part_d_output.txt
```

## Notes
- Run one test at a time.
- Restart the Pyro5 name server if old node registrations cause problems.
- Part A and B use ports 9000-9004.
- Part C uses ports 9100-9104.
- Part D uses ports 9200-9204.