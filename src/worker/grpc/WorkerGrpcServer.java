package worker.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import worker.computation.TaskExecutionEngine;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the Worker Node's embedded gRPC server.
 */
public class WorkerGrpcServer {

    private final int workerId;
    private final Server server;
    private final WorkerServiceImpl workerService;
    private final TaskExecutionEngine executionEngine;

    public WorkerGrpcServer(int workerId, int port, int threadPoolSize) {
        this.workerId = workerId;
        this.executionEngine = new TaskExecutionEngine(workerId, threadPoolSize);
        this.workerService = new WorkerServiceImpl(workerId, executionEngine);

        this.server = ServerBuilder.forPort(port)
                .addService(workerService)
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

    public TaskExecutionEngine getExecutionEngine() {
        return executionEngine;
    }
}
