# Distributed File System

A distributed file system built in C++ using MPI (Message Passing Interface) that supports file uploading, retrieval, searching, and fault tolerance through chunk-based storage with replication across multiple storage nodes.

## Architecture

The system follows a **Metadata Server + Storage Nodes** architecture:

- **Metadata Server (Rank 0):** Manages file metadata, chunk-to-node mappings, heartbeat monitoring, and coordinates all client operations.
- **Storage Nodes (Rank 1 to N-1):** Store file chunks, respond to upload/retrieve/search requests, and send periodic heartbeats to the metadata server.

### How It Works

1. Files are split into fixed-size **32-byte chunks**.
2. Each chunk is replicated across up to **3 storage nodes** (configurable via `REPLICATION_FACTOR`).
3. Chunks are distributed using a **load-balanced strategy** — nodes with fewer chunks are prioritized.
4. The metadata server tracks which chunks belong to which file and where each replica is stored.
5. Storage nodes send **heartbeats** every second; if no heartbeat is received for 3 seconds, the node is considered down.

## Features

- **Upload:** Split files into chunks and distribute replicas across active storage nodes with load balancing.
- **Retrieve:** Reassemble a file by fetching chunks from active replica nodes.
- **Search:** Find all byte-offset occurrences of a word within a file, including matches that span across chunk boundaries.
- **List File:** Display chunk distribution — shows which storage nodes hold each chunk of a file.
- **Failover:** Simulate a node failure by marking it inactive (stops heartbeat, ignores requests).
- **Recover:** Bring a failed node back online and resume heartbeat communication.
- **Heartbeat Monitoring:** Threaded heartbeat system for real-time node health tracking.

## Prerequisites

- A C++ compiler with C++11 support
- MPI implementation (e.g., OpenMPI or MPICH) with `MPI_THREAD_MULTIPLE` support
- POSIX-compatible OS (Linux/macOS)

## Build

```bash
mpic++ -w -std=c++11 DFS.cpp -pthread -o dfs
```

## Run

Launch with `N` total processes (1 metadata server + N-1 storage nodes). Maximum 12 processes supported.

```bash
mpirun -np <N> ./dfs
```

Example with 4 storage nodes:

```bash
mpirun -np 5 ./dfs
```

## Commands

All commands are entered via stdin on the metadata server (rank 0):

| Command | Syntax | Description |
|---------|--------|-------------|
| **upload** | `upload <filename> <filepath>` | Upload a file by splitting it into chunks and distributing across nodes |
| **retrieve** | `retrieve <filename>` | Retrieve and display the full contents of an uploaded file |
| **search** | `search <filename> <word>` | Search for a word in a file; returns count and byte offsets |
| **list_file** | `list_file <filename>` | List chunk distribution showing chunk index, replica count, and node ranks |
| **failover** | `failover <node_rank>` | Simulate failure of a storage node |
| **recover** | `recover <node_rank>` | Recover a previously failed storage node |
| **exit** | `exit` | Gracefully shut down all nodes and terminate |

### Output Codes

- `1` — Operation successful
- `-1` — Error (invalid input, file not found, no active nodes, etc.)

## Example Usage

```bash
# Build and run with 5 processes
mpic++ -w -std=c++11 DFS.cpp -pthread -o dfs
mpirun -np 5 ./dfs

# Upload a file
upload test1.txt testcases/test1.txt

# Retrieve the file contents
retrieve test1.txt

# Search for a word
search test1.txt Lorem

# List chunk locations
list_file test1.txt

# Simulate node 2 failure
failover 2

# Retrieve still works (chunks served by other replicas)
retrieve test1.txt

# Bring node 2 back
recover 2

# Shut down
exit
```

## Constraints

- Maximum **12** parallel MPI processes (including the metadata server)
- Maximum word size: **32 bytes** (equal to chunk size)
- Files with the same name cannot be uploaded twice

## Test Cases

The `testcases/` directory contains sample text files for testing upload, retrieve, and search operations.

## Tech Stack

- **Language:** C++11
- **Parallelism:** MPI (`MPI_THREAD_MULTIPLE`) + POSIX Threads
- **Communication:** MPI point-to-point messaging with tagged message types
