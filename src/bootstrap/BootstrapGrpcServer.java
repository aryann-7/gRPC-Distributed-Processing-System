package bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public final class BootstrapGrpcServer implements AutoCloseable {
    private final Server server;

    public BootstrapGrpcServer(int port) {
        this(port, new WorkerRegistry());
    }

    public BootstrapGrpcServer(int port, WorkerRegistry registry) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535");
        }
        server = ServerBuilder.forPort(port).addService(new BootstrapGrpcService(registry)).build();
    }

    public void start() throws IOException {
        server.start();
        System.out.println("Bootstrap gRPC listening on port " + server.getPort());
    }

    public int getPort() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public void close() {
        server.shutdown();
        try {
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: BootstrapGrpcServer [port]");
        }
        int port = args.length == 0 ? BootstrapServer.DEFAULT_PORT : Integer.parseInt(args[0]);
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Bootstrap port must be between 1 and 65535");
        }
        try (BootstrapGrpcServer bootstrap = new BootstrapGrpcServer(port)) {
            Runtime.getRuntime().addShutdownHook(new Thread(bootstrap::close, "bootstrap-shutdown"));
            bootstrap.start();
            bootstrap.awaitTermination();
        }
    }
}
