package worker.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import worker.computation.TaskExecutionEngine;
import worker.coordinator.CoordinatorServiceImpl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the Worker Node's embedded gRPC server.
 */
public class WorkerGrpcServer {

    private final int workerId;
    private final Server server;
    private final WorkerServiceImpl workerService;
    private final CoordinatorServiceImpl coordinatorService;
    private final TaskExecutionEngine executionEngine;

    public WorkerGrpcServer(int workerId, int port, int threadPoolSize) {
        this(workerId, port, threadPoolSize, null, null, null, null, null, null);
    }

    public WorkerGrpcServer(int workerId, int port, int threadPoolSize,
                            java.util.function.Supplier<worker.coordinator.CoordinatorManager> coordinatorManagerSupplier,
                            java.util.function.Supplier<Boolean> isCoordinatorSupplier,
                            java.util.function.Supplier<common.grpc.WorkerIdentity> identitySupplier) {
        this(workerId, port, threadPoolSize, coordinatorManagerSupplier, isCoordinatorSupplier, identitySupplier, null, null, null);
    }

    public WorkerGrpcServer(int workerId, int port, int threadPoolSize,
                            java.util.function.Supplier<worker.coordinator.CoordinatorManager> coordinatorManagerSupplier,
                            java.util.function.Supplier<Boolean> isCoordinatorSupplier,
                            java.util.function.Supplier<common.grpc.WorkerIdentity> identitySupplier,
                            java.util.function.Consumer<common.models.ElectionMessage> electionMessageConsumer,
                            java.util.function.Consumer<common.models.ElectionReply> electionReplyConsumer,
                            java.util.concurrent.ConcurrentMap<Integer, common.grpc.WorkerDescriptor> neighbours) {
        this(workerId, port, threadPoolSize, coordinatorManagerSupplier, isCoordinatorSupplier, identitySupplier, electionMessageConsumer, electionReplyConsumer, neighbours, null);
    }

    public WorkerGrpcServer(int workerId, int port, int threadPoolSize,
                            java.util.function.Supplier<worker.coordinator.CoordinatorManager> coordinatorManagerSupplier,
                            java.util.function.Supplier<Boolean> isCoordinatorSupplier,
                            java.util.function.Supplier<common.grpc.WorkerIdentity> identitySupplier,
                            java.util.function.Consumer<common.models.ElectionMessage> electionMessageConsumer,
                            java.util.function.Consumer<common.models.ElectionReply> electionReplyConsumer,
                            java.util.concurrent.ConcurrentMap<Integer, common.grpc.WorkerDescriptor> neighbours,
                            Runnable workerRefresher) {
        this.workerId = workerId;
        this.executionEngine = new TaskExecutionEngine(workerId, threadPoolSize);
        this.workerService = new WorkerServiceImpl(workerId, executionEngine);
        this.coordinatorService = new CoordinatorServiceImpl(
                coordinatorManagerSupplier,
                isCoordinatorSupplier,
                identitySupplier != null ? identitySupplier : () -> common.grpc.WorkerIdentity.newBuilder()
                        .setWorkerId(workerId)
                        .setSessionId("worker-" + workerId)
                        .build(),
                workerRefresher
        );

        this.server = ServerBuilder.forPort(port)
                .addService(workerService)
                .addService(coordinatorService)
                .addService(new ElectionGrpcService(electionMessageConsumer, electionReplyConsumer))
                .addService(new PeerGrpcService(workerId, neighbours != null ? neighbours : new java.util.concurrent.ConcurrentHashMap<>()))
                .build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("Worker " + workerId + " gRPC Server started on port " + getPort());
    }

    public int getPort() {
        return server.getPort();
    }

    public void stop() throws InterruptedException {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            throw e;
        } finally {
            try {
                coordinatorService.close();
            } catch (Exception ignored) {
            }
            executionEngine.shutdown();
        }
    }

    public void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public WorkerServiceImpl getWorkerService() {
        return workerService;
    }

    public CoordinatorServiceImpl getCoordinatorService() {
        return coordinatorService;
    }

    public TaskExecutionEngine getExecutionEngine() {
        return executionEngine;
    }
}
