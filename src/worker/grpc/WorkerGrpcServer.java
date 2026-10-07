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
    private final int port;
    private final Server server;
    private final WorkerServiceImpl workerService;
    private final TaskExecutionEngine executionEngine;

    public WorkerGrpcServer(int workerId, int port, int threadPoolSize) {
        this.workerId = workerId;
        this.port = port;
        this.executionEngine = new TaskExecutionEngine(workerId, threadPoolSize);
        this.workerService = new WorkerServiceImpl(workerId, executionEngine);

        this.server = ServerBuilder.forPort(port)
                .addService(workerService)
                .build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("Worker " + workerId + " gRPC Server started on port " + port);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down Worker " + workerId + " gRPC Server...");
            try {
                WorkerGrpcServer.this.stop();
            } catch (InterruptedException e) {
                e.printStackTrace(System.err);
            }
        }));
    }

    public void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        }
        if (executionEngine != null) {
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
