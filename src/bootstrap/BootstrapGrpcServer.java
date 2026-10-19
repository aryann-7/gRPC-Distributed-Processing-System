package bootstrap;

import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.LongSupplier;

public final class BootstrapGrpcServer implements AutoCloseable {
    private final Server server;
    private final BootstrapGrpcService service;
    private final WorkerRegistry registry;
    private final long expiryIntervalMillis;
    private final ScheduledExecutorService expiry;

    public BootstrapGrpcServer(int port) {
        this(port, new WorkerRegistry());
    }

    public BootstrapGrpcServer(int port, WorkerRegistry registry) {
        this(port, registry, Long.getLong("cs324.leaseDurationMs", BootstrapGrpcService.DEFAULT_LEASE_DURATION_MS),
                Long.getLong("cs324.leaseCheckIntervalMs", 1_000));
    }

    public BootstrapGrpcServer(int port, WorkerRegistry registry, long leaseDurationMillis, long expiryIntervalMillis) {
        this(port, registry, leaseDurationMillis, expiryIntervalMillis, System::currentTimeMillis);
    }

    BootstrapGrpcServer(int port, WorkerRegistry registry, long leaseDurationMillis, long expiryIntervalMillis,
                        LongSupplier wallTimeMillis) {
        if (port < 0 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 0 and 65535");
        }
        if (expiryIntervalMillis <= 0) {
            throw new IllegalArgumentException("Lease check interval must be positive");
        }
        service = new BootstrapGrpcService(registry, leaseDurationMillis, wallTimeMillis);
        this.registry = registry;
        this.expiryIntervalMillis = expiryIntervalMillis;
        expiry = Executors.newSingleThreadScheduledExecutor(task -> new Thread(task, "bootstrap-lease-expiry"));
        server = ServerBuilder.forPort(port).addService(service).build();
    }

    public synchronized void start() throws IOException {
        server.start();
        try {
            expiry.scheduleWithFixedDelay(this::expireLeases, expiryIntervalMillis, expiryIntervalMillis,
                    TimeUnit.MILLISECONDS);
        } catch (RuntimeException e) {
            close();
            throw e;
        }
        System.out.println("Bootstrap gRPC listening on port " + server.getPort());
    }

    private void expireLeases() {
        try {
            registry.expireLeases();
        } catch (RuntimeException e) {
            System.err.println("Bootstrap lease expiry scan failed: " + e.getMessage());
        }
    }

    public int getPort() {
        return server.getPort();
    }

    public void awaitTermination() throws InterruptedException {
        server.awaitTermination();
    }

    @Override
    public synchronized void close() {
        server.shutdown();
        expiry.shutdownNow();
        service.close();
        try {
            expiry.awaitTermination(5, TimeUnit.SECONDS);
            if (!server.awaitTermination(5, TimeUnit.SECONDS)) {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            server.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static final int DEFAULT_PORT = 1099;

    public static void main(String[] args) throws Exception {
        if (args.length > 1) {
            throw new IllegalArgumentException("Usage: BootstrapGrpcServer [port]");
        }
        int port = args.length == 0 ? DEFAULT_PORT : Integer.parseInt(args[0]);
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
