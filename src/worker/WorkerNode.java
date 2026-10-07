package worker;

import common.interfaces.BootstrapService;
import common.interfaces.WorkerService;
import common.models.ElectionMessage;
import common.models.ElectionReply;
import common.models.JobResult;
import common.models.JobTask;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import worker.computation.JobRunner;
import worker.coordinator.CoordinatorManager;
import worker.election.ElectionManager;

public class WorkerNode extends UnicastRemoteObject
                implements WorkerService {

        private static final long serialVersionUID = 1L;
        private final int workerId;
        private final String host;
        private final int port;
        private volatile int coordinatorId = -1;
        private final AtomicInteger currentTerm = new AtomicInteger(1);
        private final AtomicInteger jac = new AtomicInteger(0);
        private volatile boolean isCoordinator = false;
        private final ElectionManager electionManager = new ElectionManager();
        private CoordinatorManager coordinatorManager;
        private final Map<Integer, WorkerService> neighbours = new ConcurrentHashMap<>();
        private final ExecutorService computationThreadPool = Executors.newFixedThreadPool(4);
        private final Map<String, Integer> electionParents = new ConcurrentHashMap<>();
        private final Map<String, Set<Integer>> pendingReplies = new ConcurrentHashMap<>();
        private final Map<String, ElectionCandidate> bestCandidates = new ConcurrentHashMap<>();

        public WorkerNode(
                        int workerId,
                        String host,
                        int port) throws RemoteException {

                super();

                this.workerId = workerId;
                this.host = host;
                this.port = port;
        }

        /**
         * A duplicate ELECTION arriving here means this edge is a cycle in
         * the neighbour graph, not part of the spanning tree this flood
         * already built through some other path. The sender is still
         * waiting on a reply for this specific edge — without one, it waits
         * forever, since a normal duplicate is otherwise silently dropped.
         * Reply immediately with whatever candidate we currently know;
         * there's nothing further to wait on for a non-tree edge.
         */
        private void replyToCrossEdge(ElectionMessage message) {
                int senderId = message.getSenderId();
                WorkerService sender = neighbours.get(senderId);
                if (sender == null) {
                        return;
                }

                ElectionCandidate known = bestCandidates.get(message.getMessageId());
                int candidateId = (known != null) ? known.workerId : message.getCandidateId();
                int candidateJac = (known != null) ? known.jac : message.getCandidateJac();

                try {
                        sender.receiveElectionReply(new ElectionReply(
                                        message.getMessageId(),
                                        message.getTerm(),
                                        workerId,
                                        candidateId,
                                        candidateJac));
                } catch (Exception e) {
                        System.out.println("Worker " + workerId + " could not reply to cross-edge: " + e.getMessage());
                }
        }

        @Override
        public JobResult executeJob(JobTask task)
                        throws RemoteException {

                if (task == null) {

                        JobResult error = new JobResult();

                        error.setSuccess(false);
                        error.setErrorMessage(
                                        "Received null JobTask");

                        return error;
                }

                try {

                        var future = computationThreadPool.submit(
                                        new JobRunner(task));

                        return future.get();

                } catch (InterruptedException e) {

                        Thread.currentThread().interrupt();

                        JobResult error = new JobResult();

                        error.setJobId(task.getJobId());
                        error.setTaskId(task.getTaskId());
                        error.setSuccess(false);
                        error.setErrorMessage(
                                        "Task interrupted: " + e.getMessage());

                        return error;

                } catch (Exception e) {

                        JobResult error = new JobResult();

                        error.setJobId(task.getJobId());
                        error.setTaskId(task.getTaskId());
                        error.setSuccess(false);
                        error.setErrorMessage(
                                        "Execution error: " + e.getMessage());

                        return error;
                }
        }

        public void startElection() {

                int newTerm = currentTerm.incrementAndGet();

                System.out.println();
                System.out.println(
                                "====================================");

                System.out.println(
                                "Worker " + workerId +
                                                " starting ELECTION.");

                System.out.println(
                                "Term: " + newTerm);

                System.out.println(
                                "Worker JAC: " + jac.get());

                System.out.println(
                                "====================================");

                electionManager.resetElectionState();

                ElectionMessage message = electionManager.initiateElection(
                                workerId,
                                jac.get(),
                                newTerm);

                electionManager.isDuplicateAndMark(
                                message.getMessageId());

                electionParents.put(
                                message.getMessageId(),
                                -1);

                ElectionCandidate localCandidate = new ElectionCandidate(
                                workerId,
                                jac.get());

                bestCandidates.put(
                                message.getMessageId(),
                                localCandidate);

                Set<Integer> children = ConcurrentHashMap.newKeySet();

                pendingReplies.put(
                                message.getMessageId(),
                                children);

                for (Map.Entry<Integer, WorkerService> entry : neighbours.entrySet()) {

                        children.add(entry.getKey());

                        sendElection(
                                        entry.getValue(),
                                        message);
                }

                checkElectionFinished(message);
        }

        @Override
        public void receiveElectionMessage(
                        ElectionMessage message)
                        throws RemoteException {

                if (message == null) {
                        return;
                }

                if (message.getMessageId() == null
                                || message.getMessageId().isBlank()) {

                        System.out.println(
                                        "Worker " + workerId +
                                                        " rejected election message with no ID.");

                        return;
                }

                if (electionManager.isDuplicateAndMark(message.getMessageId())) {
                        System.out.println(
                                        "Worker " + workerId +
                                                        " ignored duplicate " +
                                                        message.getType() +
                                                        " message " +
                                                        message.getMessageId());

                        if (message.getType() == ElectionMessage.MessageType.ELECTION) {
                                replyToCrossEdge(message);
                        }

                        return;
                }

                if (message.getType() == ElectionMessage.MessageType.COORDINATOR) {

                        receiveCoordinatorMessage(message);
                        return;
                }

                System.out.println(
                                "Worker " + workerId +
                                                " received ELECTION " +
                                                message.getMessageId());

                int parentId = message.getSenderId();

                electionParents.put(
                                message.getMessageId(),
                                parentId);

                ElectionMessage updated = electionManager.processElectionMessage(
                                message,
                                workerId,
                                jac.get());

                bestCandidates.put(
                                message.getMessageId(),
                                new ElectionCandidate(
                                                updated.getCandidateId(),
                                                updated.getCandidateJac()));

                Set<Integer> children = ConcurrentHashMap.newKeySet();

                for (Integer neighbourId : neighbours.keySet()) {

                        if (neighbourId != parentId) {

                                children.add(neighbourId);
                        }
                }

                System.out.println(
                                "Worker " + workerId +
                                                " forwarding ELECTION " +
                                                message.getMessageId() +
                                                " to " +
                                                children.size() +
                                                " neighbour(s).");

                pendingReplies.put(
                                message.getMessageId(),
                                children);

                for (Integer neighbourId : children) {

                        WorkerService neighbour = neighbours.get(neighbourId);

                        if (neighbour != null) {

                                sendElection(
                                                neighbour,
                                                updated);
                        }
                }

                checkElectionFinished(updated);
        }

        private void sendElection(
                        WorkerService worker,
                        ElectionMessage message) {

                ElectionMessage forwarded = new ElectionMessage(
                                message.getMessageId(),
                                message.getType(),
                                message.getCandidateId(),
                                message.getCandidateJac(),
                                message.getTerm(),
                                workerId);

                try {

                        worker.receiveElectionMessage(
                                        forwarded);

                } catch (Exception e) {

                        System.out.println(
                                        "Worker " + workerId +
                                                        " could not forward ELECTION: " +
                                                        e.getMessage());
                }
        }

        @Override
        public void receiveElectionReply(
                        ElectionReply reply)
                        throws RemoteException {

                if (reply == null) {
                        return;
                }

                String electionId = reply.getMessageId();

                ElectionCandidate current = bestCandidates.get(electionId);

                ElectionCandidate received = new ElectionCandidate(
                                reply.getCandidateId(),
                                reply.getCandidateJac());

                if (current == null ||
                                isBetterCandidate(
                                                received,
                                                current)) {

                        bestCandidates.put(
                                        electionId,
                                        received);
                }

                Set<Integer> pending = pendingReplies.get(electionId);
                boolean shouldCheckFinished = false;
                ElectionMessage message = null;

                if (pending != null) {
                        synchronized (pending) {
                                pending.remove(reply.getSenderId());
                                if (pending.isEmpty()) {
                                        shouldCheckFinished = true;
                                        ElectionCandidate best = bestCandidates.get(electionId);
                                        int bId = best != null ? best.workerId : workerId;
                                        int bJac = best != null ? best.jac : jac.get();
                                        message = new ElectionMessage(
                                                        electionId,
                                                        ElectionMessage.MessageType.ELECTION,
                                                        bId,
                                                        bJac,
                                                        reply.getTerm(),
                                                        workerId);
                                }
                        }
                }

                if (shouldCheckFinished && message != null) {
                        checkElectionFinished(message);
                }
        }

        private void checkElectionFinished(
                        ElectionMessage message) {
                Set<Integer> pending = pendingReplies.get(
                                message.getMessageId());

                if (pending != null &&
                                !pending.isEmpty()) {

                        return;
                }

                ElectionCandidate best = bestCandidates.get(
                                message.getMessageId());

                if (best == null) {
                        return;
                }

                Integer parent = electionParents.get(
                                message.getMessageId());

                if (parent == null || parent == -1) {

                        System.out.println();
                        System.out.println(
                                        "Election completed.");

                        System.out.println(
                                        "Lowest JAC: " +
                                                        best.jac);

                        System.out.println(
                                        "Selected coordinator: Worker " +
                                                        best.workerId);

                        announceCoordinator(
                                        best.workerId,
                                        best.jac,
                                        message.getTerm());

                } else {

                        WorkerService parentWorker = neighbours.get(parent);

                        if (parentWorker != null) {

                                try {

                                        ElectionReply reply = new ElectionReply(
                                                        message.getMessageId(),
                                                        message.getTerm(),
                                                        workerId,
                                                        best.workerId,
                                                        best.jac);

                                        parentWorker.receiveElectionReply(
                                                        reply);

                                } catch (Exception e) {

                                        System.out.println(
                                                        "Could not return election result: "
                                                                        + e.getMessage());
                                }
                        }
                }
        }

        private boolean isBetterCandidate(
                        ElectionCandidate first,
                        ElectionCandidate second) {

                if (first.jac < second.jac) {
                        return true;
                }

                if (first.jac == second.jac
                                && first.workerId > second.workerId) {

                        return true;
                }

                return false;
        }

        private void announceCoordinator(
                        int coordinator,
                        int coordinatorJac,
                        int term) {

                ElectionMessage message = electionManager.createCoordinatorAnnouncement(
                                coordinator,
                                coordinatorJac,
                                term);

                electionManager.isDuplicateAndMark(
                                message.getMessageId());

                receiveCoordinatorMessage(message);

                for (Map.Entry<Integer, WorkerService> entry : neighbours.entrySet()) {

                        sendCoordinator(
                                        entry.getValue(),
                                        message);
                }
        }

        private void receiveCoordinatorMessage(
                        ElectionMessage message) {

                coordinatorId = message.getCandidateId();

                isCoordinator = (workerId == coordinatorId);

                System.out.println(
                                "Worker " + workerId +
                                                " now recognizes Worker " +
                                                coordinatorId +
                                                " as coordinator.");

                if (isCoordinator) {

                        System.out.println(
                                        "Worker " + workerId +
                                                        " has become the COORDINATOR.");

                        transitionToCoordinator();

                } else {

                        if (coordinatorManager != null) {

                                stepDownToWorker();
                        }
                }

                for (Map.Entry<Integer, WorkerService> entry : neighbours.entrySet()) {

                        if (entry.getKey() == message.getSenderId()) {

                                continue;
                        }

                        sendCoordinator(
                                        entry.getValue(),
                                        message);
                }
        }

        private void sendCoordinator(
                        WorkerService worker,
                        ElectionMessage message) {

                ElectionMessage forwarded = new ElectionMessage(
                                message.getMessageId(),
                                ElectionMessage.MessageType.COORDINATOR,
                                message.getCandidateId(),
                                message.getCandidateJac(),
                                message.getTerm(),
                                workerId);

                try {

                        worker.receiveElectionMessage(
                                        forwarded);

                } catch (Exception e) {

                        System.out.println(
                                        "Could not propagate COORDINATOR: "
                                                        + e.getMessage());
                }
        }

        private volatile String bootstrapHost = "localhost";
        private volatile int bootstrapPort = 1099;

        public void connectToBootstrap(
                        String bootstrapHost,
                        int bootstrapPort) {

                this.bootstrapHost = bootstrapHost;
                this.bootstrapPort = bootstrapPort;

                try {

                        BootstrapService bootstrap = (BootstrapService) Naming.lookup(
                                        "rmi://" +
                                                        bootstrapHost +
                                                        ":" +
                                                        bootstrapPort +
                                                        "/BootstrapService");

                        bootstrap.registerWorker(
                                        workerId,
                                        host,
                                        port);

                        List<String> endpoints = bootstrap.getInitialNeighbors(
                                        workerId);

                        for (String endpoint : endpoints) {

                                addNeighbourFromEndpoint(endpoint);
                        }

                        System.out.println(
                                        "Worker " + workerId +
                                                        " connected to Bootstrap.");

                        System.out.println(
                                        "Neighbours: " +
                                                        neighbours.keySet());

                } catch (Exception e) {

                        System.err.println(
                                        "Bootstrap connection failed: " +
                                                        e.getMessage());
                }
        }

        @Override
        public void addNeighbour(int neighbourId, WorkerService neighbour)
                        throws RemoteException {

                if (neighbourId <= 0) {
                        throw new RemoteException("Neighbour ID must be greater than 0: " + neighbourId);
                }
                if (neighbourId == workerId) {
                        throw new RemoteException("A worker cannot add itself as a neighbour");
                }
                if (neighbour == null) {
                        throw new RemoteException("Neighbour reference cannot be null");
                }

                neighbours.putIfAbsent(neighbourId, neighbour);
        }

        private void addNeighbourFromEndpoint(
                        String endpoint) {

                try {

                        if (endpoint == null) {
                                throw new RemoteException("Neighbour descriptor cannot be null");
                        }

                        String[] parts = endpoint.split("\\|", -1);

                        if (parts.length != 3) {
                                throw new RemoteException("Expected neighbour descriptor workerId|host|port");
                        }

                        int id = Integer.parseInt(parts[0]);

                        if (id <= 0 || id == workerId) {
                                throw new RemoteException("Invalid discovered neighbour ID: " + id);
                        }

                        String workerHost = parts[1];

                        if (workerHost.isBlank()) {
                                throw new RemoteException("Neighbour host cannot be blank");
                        }

                        int workerPort = Integer.parseInt(parts[2]);

                        if (workerPort <= 0 || workerPort > 65535) {
                                throw new RemoteException("Invalid neighbour port: " + workerPort);
                        }

                        Registry registry = LocateRegistry.getRegistry(
                                        workerHost,
                                        workerPort);

                        WorkerService worker = (WorkerService) registry.lookup(
                                        "Worker-" + id);

                        worker.addNeighbour(workerId, this);

                        addNeighbour(
                                        id,
                                        worker);

                        System.out.println(
                                        "Worker " + workerId +
                                                        " established bidirectional connection with Worker " +
                                                        id);

                } catch (Exception e) {

                        System.err.println(
                                        "Bidirectional neighbour setup failed for " +
                                                        endpoint + ": " +
                                                        e.getMessage());
                }
        }

        public synchronized void transitionToCoordinator() {

                if (coordinatorManager != null) {
                        return;
                }

                try {

                        isCoordinator = true;

                        coordinatorManager = new CoordinatorManager(
                                        jac.get());

                        coordinatorManager.setTermExpiredListener(
                                        this::stepDownToWorker);

                        // Add self as an active compute worker
                        coordinatorManager.addWorker(this);

                        // Add direct neighbours
                        for (WorkerService worker : neighbours.values()) {
                                coordinatorManager.addWorker(worker);
                        }

                        // Discover any additional reachable workers from bootstrap if available
                        try {
                                BootstrapService bootstrap = (BootstrapService) Naming.lookup(
                                                "rmi://" + bootstrapHost + ":" + bootstrapPort + "/BootstrapService");
                                List<String> endpoints = bootstrap.getInitialNeighbors(workerId);
                                for (String ep : endpoints) {
                                        addNeighbourFromEndpoint(ep);
                                }
                                for (WorkerService worker : neighbours.values()) {
                                        coordinatorManager.addWorker(worker);
                                }
                        } catch (Exception ignored) {
                        }

                        // Register / rebind CoordinatorService in both the Bootstrap registry and local
                        // worker registry
                        try {
                                Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
                                bootstrapRegistry.rebind("CoordinatorService", coordinatorManager);
                                System.out.println("Worker " + workerId
                                                + " bound CoordinatorService to Bootstrap registry (" + bootstrapHost
                                                + ":" + bootstrapPort + ").");
                        } catch (Exception e) {
                                System.err.println("Could not bind CoordinatorService to Bootstrap registry: "
                                                + e.getMessage());
                        }

                        try {
                                Registry localRegistry = LocateRegistry.getRegistry(host, port);
                                localRegistry.rebind("CoordinatorService", coordinatorManager);
                        } catch (Exception ignored) {
                        }

                        System.out.println(
                                        "Worker " + workerId +
                                                        " is now coordinator.");

                } catch (RemoteException e) {

                        System.err.println(
                                        "Failed to become coordinator: " +
                                                        e.getMessage());
                }
        }

        public synchronized void stepDownToWorker() {

                // Unbind CoordinatorService from registries upon stepping down
                try {
                        Registry bootstrapRegistry = LocateRegistry.getRegistry(bootstrapHost, bootstrapPort);
                        bootstrapRegistry.unbind("CoordinatorService");
                        System.out.println(
                                        "Worker " + workerId + " unbound CoordinatorService from Bootstrap registry.");
                } catch (Exception ignored) {
                }

                try {
                        Registry localRegistry = LocateRegistry.getRegistry(host, port);
                        localRegistry.unbind("CoordinatorService");
                } catch (Exception ignored) {
                }

                if (coordinatorManager != null) {

                        jac.set(
                                        coordinatorManager
                                                        .getPersistentJac());

                        coordinatorManager = null;
                }

                isCoordinator = false;

                System.out.println(
                                "Worker " + workerId +
                                                " stepped down from coordinator.");

                startElection();
        }

        @Override
        public boolean ping() {
                return true;
        }

        public int getWorkerId() {
                return workerId;
        }

        public int getJac() {
                return jac.get();
        }

        public boolean isCoordinator() {
                return isCoordinator;
        }

        public int getCoordinatorId() {
                return coordinatorId;
        }

        public int getCurrentTerm() {
                return currentTerm.get();
        }

        private static class ElectionCandidate {

                private final int workerId;
                private final int jac;

                ElectionCandidate(
                                int workerId,
                                int jac) {

                        this.workerId = workerId;
                        this.jac = jac;
                }
        }
}
