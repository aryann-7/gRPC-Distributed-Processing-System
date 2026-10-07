# CS324 Distributed Computing Cluster System 🖥️

**Assignment One Project for CS324 Distributed Systems Course @USP**

A fully-functional distributed computing cluster implementation using Java RMI that allows computational jobs to run in parallel across multiple worker nodes under the coordination of an automatically-elected leader.

[![Java](https://img.shields.io/badge/Java-8%2B-orange.svg)](https://www.oracle.com/java/)
[![RMI](https://img.shields.io/badge/RMI-Enabled-blue.svg)](https://docs.oracle.com/javase/tutorial/rmi/)
[![Status](https://img.shields.io/badge/Status-Complete-success.svg)](https://github.com)

## Instructions on how to configure

**Follow these 4 simple steps** (or just run `scripts\run_cluster.bat` / `scripts/run_cluster.sh`, which does all of this automatically):

1. **Start Bootstrap Server:** (Run in terminal and set directory to where the project is located)

   java -cp bin bootstrap.BootstrapServer


2. **Start Worker Nodes** (open 3 new terminals):

   java -cp bin worker.WorkerMain 101 localhost 1101 localhost 1099
   java -cp bin worker.WorkerMain 102 localhost 1102 localhost 1099
   java -cp bin worker.WorkerMain 103 localhost 1103 localhost 1099


3. **Wait for Election** (look for "Worker X has become the COORDINATOR" message)

4. **Start Client:**

   java -cp bin client.ClientMain 1


Now you can submit jobs through the GUI


## Table of Contents

- [Overview](#overview)
- [System Components](#system-components)
- [Supported Job Types](#supported-job-types)
- [Prerequisites](#prerequisites)
- [How to Compile](#how-to-compile)
- [How to Run the System](#how-to-run-the-system)
- [Using the Client GUI](#using-the-client-gui)
- [Testing](#testing)
- [Test Data Files](#test-data-files)
- [Example Workflow](#example-workflow)
- [Architecture Notes](#architecture-notes)
- [Troubleshooting](#troubleshooting)
- [Development & Version Control](#development--version-control)
- [License](#license)


## Overview

A distributed computing cluster implementation using Java RMI that allows jobs to run across multiple worker nodes under the coordination of an elected leader.

## System Components

### 1. Bootstrap Node
- Maintains registry of all active worker nodes
- Does not participate in elections or job processing
- Runs as a separate Java process

### 2. Worker Nodes
- Form an unstructured network with peer connections
- Participate in leader elections based on Job Allocation Counter (JAC)
- Execute distributed computational jobs
- Support concurrent job execution using multithreading

### 3. Coordinator (Elected Leader)
- Elected worker that accepts jobs from clients
- Distributes workload evenly among available workers
- Serves for one term (up to 5 jobs)
- New election triggered after term ends

### 4. Client (GUI Application)
- Java Swing GUI for submitting computational jobs
- Supports manual data entry and CSV file loading
- Handles concurrent job submissions within single client
- Multiple clients can run simultaneously

## Supported Job Types

1. **MAX(numbers)** - Returns the largest value from an unsorted list of numbers
2. **PRIMESUM(start, end)** - Calculates the sum of all prime numbers within the specified range
3. **PRIMECOUNT(numbers)** - Counts the occurrence of prime numbers in an unsorted list

## Prerequisites

- Java JDK 8 or higher
- Java RMI configured and enabled
- Network connectivity between all nodes (all `localhost` for single-machine testing)

## How to Compile

From the project root directory, the easiest option is to just run the cluster script (Windows: `scripts\run_cluster.bat`, Mac/Linux: `scripts/run_cluster.sh`) — it compiles everything to `bin/` automatically before launching.

To compile manually instead:

javac -d bin -sourcepath src src/bootstrap/BootstrapServer.java src/worker/WorkerMain.java src/client/ClientMain.java

Listing each entry point and using `-sourcepath src` lets `javac` automatically pull in every class

## How to Run the System

### Step 1: Start the Bootstrap Node

The Bootstrap Node must be started first as it maintains the registry of active workers.


java -cp bin bootstrap.BootstrapServer


Expected output:

================================
Bootstrap Node started.
RMI port: 1099
Service: BootstrapService
================================


To use a port other than the default 1099:

java -cp bin bootstrap.BootstrapServer 2000

If you do this every worker and client command in this README needs its `1099` changed to match the bootstrap port

### Step 2: Start Worker Nodes

Start multiple worker nodes. Each worker needs a unique ID, its own port and the bootstrap node's address.


# Start Worker 101
java -cp bin worker.WorkerMain 101 localhost 1101 localhost 1099

# Start Worker 102 (in a new terminal)
java -cp bin worker.WorkerMain 102 localhost 1102 localhost 1099

# Start Worker 103 (in a new terminal)
java -cp bin worker.WorkerMain 103 localhost 1103 localhost 1099


Command format: `java -cp bin worker.WorkerMain <workerID> <host> <workerPort> <bootstrapHost> <bootstrapPort>`

Expected output from each worker:

================================
Worker 101 started.
Port: 1101
JAC: 0
================================
Worker 101 connected to Bootstrap.
Neighbours: [...]


### Step 3: Leader Election

A few seconds after connecting to the bootstrap node each worker automatically starts its own election. You'll see `ELECTION` messages flood between workers, converging on a single result:


Election completed.
Lowest JAC: 0
Selected coordinator: Worker 103
Worker 103 has become the COORDINATOR.


Election criteria: lowest JAC wins; if JAC is tied, the highest worker ID wins.

### Step 4: Start Client GUIs

Once a coordinator is elected, clients can connect and submit jobs.


# Start Client 1
java -cp bin client.ClientMain 1

# Start Client 2 (in a new terminal, for testing multiple concurrent clients)
java -cp bin client.ClientMain 2


Command format: `java -cp bin client.ClientMain <clientID>`

## Using the Client GUI

### Connection Setup
1. Enter the coordinator host (default: `localhost`)
2. Enter the coordinator port (default: `1099` — this is the bootstrap/RMI registry port, not the coordinator's own worker port)
3. Click "Test Discovery" to verify connection to the coordinator

### Submitting Jobs

#### For MAX or PRIMECOUNT:
1. Select job type from dropdown (MAX or PRIMECOUNT)
2. **Option A - Manual Entry:**
   - Enter comma-separated numbers in the text area
   - Example: `10, 25, 3, 99, 17, 4, 101, 88, 2, 7`
3. **Option B - Load from CSV:**
   - Click "Browse CSV File..."
   - Select a CSV file containing numbers (see `data`)
4. Click "Submit Job (Async)"

#### For PRIMESUM:
1. Select "PRIMESUM" from dropdown
2. Enter Start value (e.g., `1`)
3. Enter End value (e.g., `1000`)
4. Click "Submit Job (Async)"

### Concurrent Job Submission
- You can submit multiple jobs rapidly without waiting for previous jobs to complete
- The GUI remains responsive during job execution
- Results appear in the output log with timestamps

### Multiple Concurrent Clients
- Run multiple client processes simultaneously
- Each client can submit jobs independently
- All jobs are handled by the same coordinator and distributed across workers

## Testing

An automated end-to-end integration test is included at `tests/client/ClientJobEndToEndTest.java`. It spins up a real bootstrap node and 3 real workers in-process, triggers a genuine flooding election, and verifies:
- A single client submitting multiple jobs concurrently via a thread pool
- Multiple independent clients submitting jobs to the coordinator simultaneously
- Correct recovery across a real 5-job coordinator term rotation and re-election

This test compiles to a separate `out/` folder, independent of the cluster's own `bin/` folder — it builds and runs entirely on its own and doesn't require the cluster to already be running.

Run it with:

javac -d out -sourcepath "src;tests" tests/client/ClientJobEndToEndTest.java
java -cp out client.ClientJobEndToEndTest

(On Mac/Linux, use `"src:tests"` instead of `"src;tests"`.)

A successful run ends with `All end-to-end client/coordinator integration tests passed.`

Two smaller unit/integration tests also exist under `tests/bootstrap/` and `tests/worker/`, covering bootstrap registration and worker-to-worker connection setup in isolation.

## Test Data Files

Two CSV test files are provided in `data/`:

1. **sample_max.csv** - Sample numerical data for testing MAX jobs
2. **sample_primecount.csv** - Sample numerical data for testing PRIMECOUNT jobs

## Example Workflow


# Terminal 1: Bootstrap
java -cp bin bootstrap.BootstrapServer

# Terminal 2-4: Workers
java -cp bin worker.WorkerMain 101 localhost 1101 localhost 1099
java -cp bin worker.WorkerMain 102 localhost 1102 localhost 1099
java -cp bin worker.WorkerMain 103 localhost 1103 localhost 1099

# Wait for election to complete and coordinator to be elected

# Terminal 5-6: Clients
java -cp bin client.ClientMain 1
java -cp bin client.ClientMain 2

# Use the GUI to submit jobs and see distributed computation in action

## Architecture Notes

### Leader Election Algorithm
- Custom flooding-based election algorithm propagates ELECTION messages through the unstructured network
- Each message carries a unique ID; every worker tracks IDs it has already processed to prevent reprocessing and infinite loops
- All reachable active workers participate in the election
- Election criteria: Lowest JAC, then highest ID as tiebreaker
- COORDINATOR message propagated after election to inform all workers
- System eventually reaches consensus on a single coordinator per term

### Job Distribution
The coordinator divides work evenly among available workers:
- **PRIMESUM(1, 1000)** with 3 workers:
  - Worker 101: PRIMESUM(1, 334)
  - Worker 102: PRIMESUM(335, 667)
  - Worker 103: PRIMESUM(668, 1000)

### Concurrent Execution & Thread Safety
- Each worker uses a fixed thread pool (`JobRunner implements Callable<JobResult>`) for concurrent computation of assigned sub-tasks
- The coordinator uses a cached thread pool to dispatch tasks to all assigned workers concurrently, then aggregates each `Future<JobResult>`
- The client uses its own cached thread pool (`clientSubmissionPool`) so submitting a job never blocks the GUI, allowing overlapping submissions
- Shared election/coordinator state (`neighbours`, `seenMessageIds`, `pendingReplies`, JAC/term counters) uses `ConcurrentHashMap` and `AtomicInteger` for safe concurrent access across RMI call threads
- `CoordinatorManager.submitJob()` is `synchronized` to prevent two concurrent submissions from interleaving the term-expiry check with job processing
- Term-expiry notification is additionally guarded with an `AtomicBoolean.compareAndSet`, ensuring that if several concurrent submissions land on an already-expired coordinator, only the first one triggers step-down/re-election — otherwise a second trigger could unbind a newly-elected coordinator's registry entry moments after it bound

## Troubleshooting

### Client cannot connect to coordinator
- Ensure Bootstrap Node is running first
- Ensure at least one worker is running and elected as coordinator
- Check that host/port settings in client GUI match the bootstrap configuration
- Verify no firewall blocking RMI communication

### Workers cannot find bootstrap node
- Ensure Bootstrap Node is started before workers
- Check that bootstrap host and port are correct in worker startup command
- Verify RMI registry is accessible

### Jobs not executing
- Ensure coordinator has been elected (check worker terminal output)
- Verify workers are actively running and connected
- Check for errors in worker or coordinator terminal output

### "Could not locate active CoordinatorService" right after submitting a 5th job
- This is expected, briefly: the coordinator's term just expired and a new election is running. Wait a few seconds and retry — the client GUI does not currently auto-retry this itself, so a manual resubmission is needed once a new coordinator is announced in the worker terminals.

## Development & Version Control

This project uses Git for version control with separate feature branches:
- `feature/section1-bootstrap` - Bootstrap Node implementation
- `feature/section2-workers` - Worker Node implementation
- `feature/section3-leader-election` - Leader Election implementation
- `feature/section4-client-gui` - Client GUI implementation (this section)

## License

Academic project for CS324 course at University of the South Pacific