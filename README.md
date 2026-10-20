# CS324 Distributed Computing Cluster System

Assignment Two Project for CS324 Distributed Systems Course @ USP.

A fault-tolerant distributed computing cluster implementation using gRPC and Protocol Buffers that executes computational jobs in parallel across multiple worker nodes under the coordination of an automatically-elected leader.

## Instructions on how to configure and run

Follow these simple steps (or run scripts\run_cluster.bat, which does all of this automatically):

1. Build Project Packages via Maven:

   ```bash
   mvn clean package -DskipTests
   cd client && mvn clean package -DskipTests && cd ..
   ```

2. Start Bootstrap Server:

   ```bash
   java -jar target\bootstrap-server.jar 50050
   ```

3. Start Worker Nodes (open 3 new terminals):

   ```bash
   java -jar target\worker-node.jar 101 localhost 1101 localhost 50050 50051
   java -jar target\worker-node.jar 102 localhost 1102 localhost 50050 50052
   java -jar target\worker-node.jar 103 localhost 1103 localhost 50050 50053
   ```

4. Wait for Election:

   Watch the worker terminals for the message:
   `Worker 103 has become the COORDINATOR.`

5. Start Client GUI instances:

   ```bash
   java -jar client\target\distrilab-client.jar 1 localhost 50050
   java -jar client\target\distrilab-client.jar 2 localhost 50050
   ```

Now you can submit jobs through the GUI.

To dynamically add more workers or clients while jobs are running:

- Run `scripts\add_worker.bat` to automatically spawn a new worker.
- Run `scripts\add_client.bat` to automatically spawn a new client GUI.

## Overview

A distributed computing cluster implementation using pure gRPC and Protocol Buffers (zero Java RMI) that allows jobs to run across multiple worker nodes under the coordination of an elected leader, with full worker and coordinator failure tolerance and real-time streaming progress.

## System Components

### 1. Bootstrap Node (BootstrapGrpcServer)

- Implements BootstrapServiceGrpc.
- Maintains a registry of active worker nodes with dynamic lease renewals and heartbeats.
- Streams live cluster membership changes to the coordinator via gRPC server-streaming RPC.
- Serves as the dynamic discovery directory for active coordinator lookups.
- Runs as an independent Java process on port 50050.

### 2. Worker Nodes (WorkerMain / WorkerNode)

- Form an unstructured peer-to-peer network via PeerServiceGrpc.
- Participate in leader election via ElectionServiceGrpc.
- Execute sub-tasks concurrently using multithreaded task execution engines.
- Stream real-time computation progress to the coordinator and handle task cancellation tokens.
- Run as separate Java processes on unique gRPC ports (e.g., 50051, 50052, 50053).

### 3. Coordinator (Elected Leader)

- Elected worker node that exposes CoordinatorServiceGrpc.
- Accepts jobs from multiple clients concurrently.
- Decomposes computational jobs into granular chunk tasks and dispatches them across active workers.
- Persists job progress state to binary checkpoints on disk to survive failure.
- Automatically detects worker crashes and reassigns unfinished tasks to surviving workers.
- Serves for one term (up to 5 jobs), then steps down to trigger a new election.

### 4. Client (DistrilabClientApp / GUI Application)

- Java Swing GUI application built in a separate Maven project module (client/).
- Uses RetryingCoordinatorClient to query Bootstrap for the active coordinator and dynamically reconnect upon leader rotation or failover.
- Displays live job completion percentage using JTable progress bars driven by gRPC server streaming (WatchJob).
- Allows clicking any progress bar to view an active worker-level breakdown dialog showing tasks assigned, completed, and percentages per worker.

## Supported Job Types

- MAX(numbers) - Returns the largest value from an unsorted list of numbers.
- PRIMESUM(start, end) - Calculates the sum of all prime numbers within the specified range.
- PRIMECOUNT(numbers) - Counts the occurrence of prime numbers in an unsorted list.

## Prerequisites

- Java JDK 17 or higher
- Apache Maven 3.8 or higher
- Network connectivity between nodes (localhost for single-machine testing)

## How to Compile

Run Maven from the root directory to generate Protobuf classes and package shaded executable JARs:

```bash
mvn clean package -DskipTests
cd client
mvn clean package -DskipTests
cd ..
```

This creates:

- `target\bootstrap-server.jar`
- `target\worker-node.jar`
- `client\target\distrilab-client.jar`

## How to Run the System

### Step 1: Start the Bootstrap Node

The Bootstrap Node must be started first to maintain the worker registry and coordinator directory:

```bash
java -jar target\bootstrap-server.jar 50050
```

Expected output:
`Bootstrap gRPC listening on port 50050`

### Step 2: Start Worker Nodes

Start multiple worker nodes in separate terminals:

```bash
java -jar target\worker-node.jar 101 localhost 1101 localhost 50050 50051
java -jar target\worker-node.jar 102 localhost 1102 localhost 50050 50052
java -jar target\worker-node.jar 103 localhost 1103 localhost 50050 50053
```

Command format:

```text
java -jar target\worker-node.jar [workerID] [host] [internalPort] [bootstrapHost] [bootstrapPort] [grpcPort]
```

Expected output:

```text
Worker 101 gRPC Server started on port 50051
Worker 101 connected to Bootstrap via gRPC (port 50050).
```

### Step 3: Leader Election

After connecting to the Bootstrap node, workers trigger an election. ELECTION messages propagate across peers, converging on a single coordinator:

```text
Selected coordinator: Worker 103
Worker 103 has become the COORDINATOR.
```

Election criteria: lowest Job Allocation Counter (JAC), with highest worker ID as the tiebreaker.

### Step 4: Start Client GUIs

Once a coordinator is elected, launch the client applications:

```bash
java -jar client\target\distrilab-client.jar 1 localhost 50050
java -jar client\target\distrilab-client.jar 2 localhost 50050
```

Command format:

```text
java -jar client\target\distrilab-client.jar [clientID] [bootstrapHost] [bootstrapPort]
```

## Using the Client GUI

Connection Setup:

- Enter Bootstrap Host (default: localhost).
- Enter Bootstrap Port (default: 50050).
- Click "Test Discovery" to query the active coordinator through Bootstrap.

Submitting Jobs:

- For MAX or PRIMECOUNT:
  1. Select job type from the dropdown.
  2. Option A - Manual Entry: Enter comma-separated numbers in the text area.
  3. Option B - Load from CSV: Click "Browse CSV File..." and select a CSV file from data/.
  4. Click "Submit Job (Async)".
- For PRIMESUM:
  1. Select "PRIMESUM" from the dropdown.
  2. Enter Start value (e.g., 1) and End value (e.g., 20000).
  3. Click "Submit Job (Async)".

Real-Time Progress & Worker Breakdown:

- Submitted jobs appear in the Active Jobs table.
- Progress bars update in real time based on actual sub-tasks completed using gRPC streaming.
- Double-click or select any job row to open the Worker Breakdown dialog, which details the progress and task distribution of individual workers.

Concurrent Submissions:

- Clients submit jobs asynchronously without blocking the UI.
- Multiple independent client instances can submit jobs to the same coordinator simultaneously.

## Testing

Run the automated test suite with Maven:

Root module (118 tests covering Bootstrap, Worker execution, Bully election, gRPC dispatch, recovery):

```bash
mvn test
```

Client module (35 tests covering retry client, gRPC streaming, progress tracking, protobuf mapping):

```bash
cd client
mvn test
cd ..
```

All 153 unit and integration tests run purely on gRPC in-process channels or local ephemeral ports without any Java RMI dependency.

## Test Data Files

Two CSV test files are provided in data/:

- `data\sample_max.csv` - Sample numbers for testing MAX jobs.
- `data\sample_primecount.csv` - Sample numbers for testing PRIMECOUNT jobs.

## Example Workflow

Terminal 1:

```cmd
scripts\run_cluster.bat
```

This automatically builds the project, launches Bootstrap on port 50050, launches 3 workers (101-103), and opens 2 client GUIs.

Terminal 2:

```cmd
scripts\add_worker.bat
```

Dynamically joins Worker 104 into the live running mesh.

Terminal 3:

```cmd
scripts\add_client.bat
```

Dynamically opens Client GUI #3 connected to the cluster.

## Architecture Notes

Leader Election:

- Implemented via ElectionServiceGrpc (Elect, CoordinatorAnnounce, StepDown).
- Floods election requests across peers using unique message IDs to suppress duplicates.
- Winner registers its endpoint with Bootstrap via RegisterCoordinator.
- Expiration policy: Coordinator steps down after completing 5 jobs, triggering an automatic re-election.

Failure Tolerance:

- Worker Failure: If an in-flight gRPC call to a worker fails with an UNAVAILABLE or deadline exceeded status, CoordinatorManager flags the worker, drops it from the active worker set, and reassigns unfinished tasks to healthy workers.
- Coordinator Failure: If the coordinator crashes or stops responding, workers detect missing heartbeats and start a new election. The newly elected coordinator reconstructs the job state from disk checkpoints and uncompleted tasks continue without requiring the cluster to restart.
- Client Reconnection: RetryingCoordinatorClient intercepts broken streams or transient errors and re-queries Bootstrap to find the new coordinator, seamlessly resuming job tracking.

Thread Safety:

- Multi-threaded task execution engines use fixed and cached thread pools for parallel chunk computation.
- Thread-safe collections (ConcurrentHashMap, CopyOnWriteArrayList, AtomicInteger, AtomicBoolean) protect coordinator state, peer tables, and progress listeners under high concurrency.

## Troubleshooting

Client cannot connect to coordinator:

- Ensure Bootstrap Server is running on port 50050.
- Ensure at least 3 workers are running and an election has settled.
- Check that client is pointing to the correct Bootstrap host and port.

Workers cannot find bootstrap server:

- Ensure Bootstrap Server is started before workers.
- Confirm port 50050 is not blocked or occupied by another process.

Coordinator mid-term election:

- When a coordinator reaches its 5-job limit, it steps down and an election begins. The client GUI automatically retries discovery against Bootstrap and reconnects to the newly elected coordinator.

Port conflicts:

- Use scripts\add_worker.bat to automatically compute free gRPC port numbers.
- Verify default ports (50050 for Bootstrap, 50051+ for workers) are available.
