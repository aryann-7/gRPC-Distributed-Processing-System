package worker;

import common.interfaces.WorkerService;
import common.models.ElectionMessage;
import common.models.ElectionReply;
import common.models.JobResult;
import common.models.JobTask;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import worker.computation.JobRunner;
import worker.computation.TaskExecutionEngine;
import worker.coordinator.CoordinatorManager;
import worker.election.ElectionManager;
import worker.grpc.WorkerGrpcServer;

public class WorkerNode implements WorkerService {

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
        private final WorkerGrpcServer grpcServer;
        private final TaskExecutionEngine executionEngine;
        private final AtomicBoolean stopped = new AtomicBoolean();
        private final String sessionId = java.util.UUID.randomUUID().toString();

        public WorkerNode(
                        int workerId,
                        String host,
                        int port) {
                this(workerId, host, port, 0);
        }

        public WorkerNode(int workerId, String host, int port, int grpcPort) {

                this.workerId = workerId;
                this.host = host;
                this.port = port;

                if (port < 1 || port > 65535 || grpcPort < 0 || grpcPort > 65535 || grpcPort == port) {
                        throw new IllegalArgumentException("Worker ports require distinct valid range 1-65535; gRPC may use 0");
                }

                WorkerGrpcServer startingServer = null;
                try {
                        startingServer = new WorkerGrpcServer(
                                workerId,
                                grpcPort,
                                4,
                                () -> this.coordinatorManager,
                                () -> this.isCoordinator,
                                () -> common.grpc.WorkerIdentity.newBuilder()
                                        .setWorkerId(workerId)
                                        .setSessionId(sessionId)
                                        .build()
                        );
                        startingServer.start();
                } catch (Exception e) {
                        computationThreadPool.shutdownNow();
                        if (startingServer != null) {
                                try {
                                        startingServer.stop();
                                } catch (InterruptedException interrupted) {
                                        Thread.currentThread().interrupt();
                                        e.addSuppressed(interrupted);
                                }
                        }
                        throw new IllegalStateException("Worker " + workerId + " gRPC startup failed on port " + grpcPort, e);
                }
                this.grpcServer = startingServer;
                this.executionEngine = startingServer.getExecutionEngine();
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
        public JobResult executeJob(JobTask task) {

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
                        ElectionMessage message) {

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
                        ElectionReply reply) {

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
        private volatile int bootstrapPort = 50050;

        public void connectToBootstrap(
                        String bootstrapHost,
                        int bootstrapPort) {

                this.bootstrapHost = bootstrapHost;
                this.bootstrapPort = bootstrapPort;

                // Pure gRPC Registration with Bootstrap
                try (worker.grpc.BootstrapGrpcClient grpcClient = new worker.grpc.BootstrapGrpcClient(bootstrapHost, bootstrapPort)) {
                        common.grpc.WorkerDescriptor descriptor = common.grpc.WorkerDescriptor.newBuilder()
                                .setIdentity(common.grpc.WorkerIdentity.newBuilder()
                                        .setWorkerId(workerId)
                                        .setSessionId(sessionId)
                                        .build())
                                .setEndpoint(common.grpc.WorkerEndpoint.newBuilder()
                                        .setHost(host)
                                        .setPort(grpcServer != null ? grpcServer.getPort() : port)
                                        .build())
                                .build();

                        var regResponse = grpcClient.registerWorker(descriptor);
                        var neighboursList = grpcClient.getInitialNeighbours(descriptor.getIdentity(), regResponse.getRegistryEpoch());

                        for (common.grpc.WorkerDescriptor nDesc : neighboursList) {
                                int nId = nDesc.getIdentity().getWorkerId();
                                String nHost = nDesc.getEndpoint().getHost();
                                int nPort = nDesc.getEndpoint().getPort();
                                addNeighbourFromEndpoint(nId + "|" + nHost + "|" + nPort);
                        }

                        System.out.println("Worker " + workerId + " connected to Bootstrap via gRPC (port " + bootstrapPort + ").");
                        System.out.println("Neighbours: " + neighbours.keySet());
                } catch (Exception grpcEx) {
                        System.err.println("Worker " + workerId + " failed to connect to Bootstrap via gRPC: " + grpcEx.getMessage());
                }
        }

        @Override
        public void addNeighbour(int neighbourId, WorkerService neighbour) {

                if (neighbourId <= 0) {
                        throw new IllegalArgumentException("Neighbour ID must be greater than 0: " + neighbourId);
                }
                if (neighbourId == workerId) {
                        throw new IllegalArgumentException("A worker cannot add itself as a neighbour");
                }
                if (neighbour == null) {
                        throw new IllegalArgumentException("Neighbour reference cannot be null");
                }

                neighbours.putIfAbsent(neighbourId, neighbour);
        }

        private void addNeighbourFromEndpoint(
                        String endpoint) {

                try {

                        if (endpoint == null) {
                                throw new IllegalArgumentException("Neighbour descriptor cannot be null");
                        }

                        String[] parts = endpoint.split("\\|", -1);

                        if (parts.length != 3) {
                                throw new IllegalArgumentException("Expected neighbour descriptor workerId|host|port");
                        }

                        int id = Integer.parseInt(parts[0]);

                        if (id <= 0 || id == workerId) {
                                throw new IllegalArgumentException("Invalid discovered neighbour ID: " + id);
                        }

                        String workerHost = parts[1];

                        if (workerHost.isBlank()) {
                                throw new IllegalArgumentException("Neighbour host cannot be blank");
                        }

                        int workerPort = Integer.parseInt(parts[2]);

                        if (workerPort <= 0 || workerPort > 65535) {
                                throw new IllegalArgumentException("Invalid neighbour port: " + workerPort);
                        }

                        // Connecting via pure gRPC peer client
                        common.grpc.WorkerDescriptor desc = common.grpc.WorkerDescriptor.newBuilder()
                                .setIdentity(common.grpc.WorkerIdentity.newBuilder()
                                        .setWorkerId(id)
                                        .setSessionId("worker-" + id)
                                        .build())
                                .setEndpoint(common.grpc.WorkerEndpoint.newBuilder()
                                        .setHost(workerHost)
                                        .setPort(workerPort)
                                        .build())
                                .build();

                        worker.grpc.WorkerGrpcClient grpcClient = new worker.grpc.WorkerGrpcClient(
                                desc,
                                candId -> common.grpc.WorkerIdentity.newBuilder().setWorkerId(candId).setSessionId("worker-" + candId).build()
                        );

                        if (grpcClient.ping()) {
                                addNeighbour(id, grpcClient);
                                System.out.println("Worker " + workerId + " established gRPC neighbour connection with Worker " + id);
                        } else {
                                grpcClient.close();
                        }

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
                                        workerId,
                                        jac.get(),
                                        currentTerm.get());

                        coordinatorManager.setTermExpiredListener(
                                        this::stepDownToWorker);

                        // Add self as an active compute worker
                        coordinatorManager.addWorker(this);

                        // Add direct neighbours
                        for (WorkerService worker : neighbours.values()) {
                                coordinatorManager.addWorker(worker);
                        }

                        // 1. Publish Coordinator to Bootstrap via pure gRPC
                        io.grpc.ManagedChannel bsChannel = null;
                        try {
                                bsChannel = io.grpc.ManagedChannelBuilder.forAddress(bootstrapHost, bootstrapPort).usePlaintext().build();
                                common.grpc.BootstrapServiceGrpc.BootstrapServiceBlockingStub bsStub = common.grpc.BootstrapServiceGrpc.newBlockingStub(bsChannel);
                                bsStub.withDeadlineAfter(3, java.util.concurrent.TimeUnit.SECONDS).publishCoordinator(
                                        common.grpc.PublishCoordinatorRequest.newBuilder()
                                                .setCoordinator(common.grpc.CoordinatorRecord.newBuilder()
                                                        .setCoordinator(common.grpc.WorkerDescriptor.newBuilder()
                                                                .setIdentity(common.grpc.WorkerIdentity.newBuilder()
                                                                        .setWorkerId(workerId)
                                                                        .setSessionId(sessionId)
                                                                        .build())
                                                                .setEndpoint(common.grpc.WorkerEndpoint.newBuilder()
                                                                        .setHost(host)
                                                                        .setPort(grpcServer != null ? grpcServer.getPort() : port)
                                                                        .build())
                                                                .build())
                                                        .setTerm(currentTerm.get())
                                                        .build())
                                                .build());
                                System.out.println("Worker " + workerId + " published Coordinator identity to Bootstrap via gRPC.");
                        } catch (Exception e) {
                                System.err.println("Could not publish coordinator to bootstrap via gRPC: " + e.getMessage());
                        } finally {
                                if (bsChannel != null) {
                                        bsChannel.shutdown();
                                }
                        }

                        System.out.println(
                                        "Worker " + workerId +
                                                        " is now coordinator.");

                } catch (Exception e) {

                        System.err.println(
                                        "Failed to become coordinator: " +
                                                        e.getMessage());
                }
        }

        public synchronized void stepDownToWorker() {

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

        public WorkerGrpcServer getGrpcServer() {
                return grpcServer;
        }

        public TaskExecutionEngine getExecutionEngine() {
                return executionEngine;
        }

        public void stop() {
                if (!stopped.compareAndSet(false, true)) {
                        return;
                }
                try {
                        grpcServer.stop();
                } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                } finally {
                        computationThreadPool.shutdownNow();
                        if (coordinatorManager != null) {
                                coordinatorManager.close();
                        }
                }
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

