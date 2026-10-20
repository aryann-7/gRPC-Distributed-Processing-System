# Distributed Computing Cluster Startup Guide

Quick reference and execution guide for launching and scaling the gRPC-based distributed computing cluster.

## Quick Reference Commands

All commands can be executed from the repository root directory.

| Scenario                            | PowerShell / Terminal Command    | Command Prompt (cmd)           | Notes                                              |
| :---------------------------------- | :------------------------------- | :----------------------------- | :------------------------------------------------- |
| Run Cluster (Default)               | `.\scripts\run_cluster.bat`      | `scripts\run_cluster.bat`      | Starts 3 workers (101-103) and 2 client GUIs       |
| Run Cluster (5 Workers, 3 Clients)  | `.\scripts\run_cluster.bat 5 3`  | `scripts\run_cluster.bat 5 3`  | Starts 5 workers (101-105) and 3 client GUIs       |
| Run Cluster (10 Workers, 3 Clients) | `.\scripts\run_cluster.bat 10 3` | `scripts\run_cluster.bat 10 3` | Starts 10 workers (101-110) and 3 client GUIs      |
| Add 1 Worker Dynamically            | `.\scripts\add_worker.bat`       | `scripts\add_worker.bat`       | Spawns next worker (auto-increments ID and port)   |
| Add 1 Client Dynamically            | `.\scripts\add_client.bat`       | `scripts\add_client.bat`       | Spawns next Client GUI (auto-increments client ID) |

## Parameter Details

The main cluster script `run_cluster.bat` accepts arguments in the following order:

```bat
run_cluster.bat [num_workers] [num_clients]
```

- `num_workers` (Optional, default: 3): Number of worker nodes to launch (min: 3, max: 10).
- `num_clients` (Optional, default: 2): Number of Client GUI instances to launch.

Note: The script automatically triggers a Maven build (`mvn clean package -DskipTests=true`) for both the main engine and the client module before launching nodes.

## 1. Running Cluster (Default: 3 Workers and 2 Clients)

Launches the Bootstrap node, 3 worker nodes (IDs 101, 102, 103), and 2 client GUI windows. Worker 103 is elected as the initial coordinator.

### Default Cluster Command

PowerShell:

```powershell
.\scripts\run_cluster.bat
```

Command Prompt:

```cmd
scripts\run_cluster.bat
```

## 2. Running Cluster with 5 Workers and 3 Clients

Launches the Bootstrap node, 5 worker nodes (IDs 101 to 105), and 3 client GUI windows. Worker 105 is elected as the initial coordinator.

### 5-Worker Cluster Command

PowerShell:

```powershell
.\scripts\run_cluster.bat 5 3
```

Command Prompt:

```cmd
scripts\run_cluster.bat 5 3
```

## 3. Running Cluster with 10 Workers and 3 Clients

Launches the full 10-node worker cluster (IDs 101 to 110) and 3 client GUI windows. Worker 110 is elected as the initial coordinator.

### 10-Worker Cluster Command

PowerShell:

```powershell
.\scripts\run_cluster.bat 10 3
```

Command Prompt:

```cmd
scripts\run_cluster.bat 10 3
```

## 4. Dynamically Adding a Worker Node

You can add a worker at any time while the cluster is active (even while jobs are executing). The new worker connects to the peer network, registers with the Bootstrap server, and is immediately available to the coordinator for job distribution.

### Auto-Increment Worker Launch

Automatically detects the highest running worker ID from `cluster_state.txt` and starts the next worker with the appropriate port:

PowerShell:

```powershell
.\scripts\add_worker.bat
```

Command Prompt:

```cmd
scripts\add_worker.bat
```

### Manual Worker Override

To manually specify the worker parameters:

```bat
scripts\add_worker.bat [worker_id] [grpc_port] [bootstrap_host] [bootstrap_port]
```

Example (Worker 106 on port 50056):

```powershell
.\scripts\add_worker.bat 106 50056 localhost 50050
```

## 5. Dynamically Adding a Client GUI

You can launch additional client GUIs at any time to submit concurrent jobs from separate clients.

### Auto-Increment Client Launch

Automatically detects the highest running client ID from `client_state.txt` and opens a new GUI with the next ID:

PowerShell:

```powershell
.\scripts\add_client.bat
```

Command Prompt:

```cmd
scripts\add_client.bat
```

### Manual Client Override

To manually specify client parameters:

```bat
scripts\add_client.bat [client_id] [bootstrap_host] [bootstrap_port]
```

Example (Client GUI #4):

```powershell
.\scripts\add_client.bat 4 localhost 50050
```

## Network and Port Allocation Map

| Component        | Node ID     | gRPC Port   | Purpose                                               |
| :--------------- | :---------- | :---------- | :---------------------------------------------------- |
| Bootstrap Server | -           | 50050       | Node discovery, registry, and heartbeat tracking      |
| Worker 101       | 101         | 50051       | Computation worker                                    |
| Worker 102       | 102         | 50052       | Computation worker                                    |
| Worker 103       | 103         | 50053       | Computation worker (Initial Leader in 3-worker setup) |
| Worker 104       | 104         | 50054       | Computation worker                                    |
| Worker 105       | 105         | 50055       | Computation worker (Initial Leader in 5-worker setup) |
| Worker 106-110   | 106-110     | 50056-50060 | Scaled computation workers                            |
| Client GUIs      | #1, #2, ... | Ephemeral   | Submits jobs and displays live task breakdown         |

## Stopping the Cluster and Cleanup

### Step 1: Close Application Windows

Close each node terminal window or client GUI window.

### Step 2: Force Kill All Java Processes

PowerShell:

```powershell
Stop-Process -Name "java" -Force
```

Command Prompt:

```cmd
taskkill /F /IM java.exe
```

### Step 3: Reset Cluster State

To start fresh from ID 101 and Client 1, delete state tracking files if needed:

PowerShell:

```powershell
Remove-Item scripts\cluster_state.txt, scripts\client_state.txt, scripts\clients_registry.txt -ErrorAction SilentlyContinue
```

Command Prompt:

```cmd
del scripts\cluster_state.txt scripts\client_state.txt scripts\clients_registry.txt 2>nul
```
