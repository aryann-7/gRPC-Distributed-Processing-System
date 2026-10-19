package worker;

import bootstrap.BootstrapGrpcServer;
import bootstrap.WorkerRegistry;
import common.grpc.HeartbeatRequest;
import common.grpc.WorkerServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import worker.grpc.WorkerGrpcServer;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerStartupTest {
    private final List<WorkerNode> workers = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();
    private BootstrapGrpcServer bootstrapServer;

    @AfterEach
    public void tearDown() throws Exception {
        for (ManagedChannel channel : channels) {
            try {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
        workers.forEach(WorkerNode::stop);
        if (bootstrapServer != null) {
            bootstrapServer.close();
        }
    }

    @Test
    public void grpcServesWorkerAndStopsCleanly() throws Exception {
        int workerPort;
        try (ServerSocket socket = new ServerSocket(0)) {
            workerPort = socket.getLocalPort();
        }
        WorkerNode worker = worker(201, workerPort);
        assertTrue(worker.ping());
        int grpcPort = worker.getGrpcServer().getPort();
        assertTrue(grpcPort > 0);
        assertNotEquals(workerPort, grpcPort);
        var stub = grpcStub(grpcPort);
        assertEquals(201, stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
        worker.stop();
        worker.stop();

        WorkerGrpcServer replacement = new WorkerGrpcServer(202, grpcPort, 1);
        try {
            replacement.start();
        } finally {
            replacement.stop();
        }
    }

    @Test
    public void multipleWorkersReceiveDistinctGrpcPorts() throws Exception {
        int port1, port2;
        try (ServerSocket s1 = new ServerSocket(0); ServerSocket s2 = new ServerSocket(0)) {
            port1 = s1.getLocalPort();
            port2 = s2.getLocalPort();
        }
        WorkerNode first = worker(201, port1), second = worker(202, port2);
        assertNotEquals(first.getGrpcServer().getPort(), second.getGrpcServer().getPort());
        assertEquals(201, grpcStub(first.getGrpcServer().getPort()).withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
        assertEquals(202, grpcStub(second.getGrpcServer().getPort()).withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
    }

    @Test
    public void occupiedOrIdenticalGrpcPortFailsConstructionClearly() throws Exception {
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        try (ServerSocket occupied = new ServerSocket(0)) {
            IllegalStateException error = assertThrows(IllegalStateException.class,
                    () -> new WorkerNode(201, "localhost", port, occupied.getLocalPort()));
            assertTrue(error.getMessage().contains("gRPC startup failed"));
            assertNotNull(error.getCause());
        }
        assertThrows(IllegalArgumentException.class, () -> new WorkerNode(201, "localhost", port, port));
    }

    @Test
    public void mainStartupFailureHandlesOccupiedPortGracefully() throws Exception {
        int port;
        try (ServerSocket reservation = new ServerSocket(0)) {
            port = reservation.getLocalPort();
        }
        try (ServerSocket occupied = new ServerSocket(0)) {
            assertThrows(IllegalStateException.class, () -> WorkerMain.main(new String[] {
                    "201", "localhost", Integer.toString(port), "localhost", "50050",
                    Integer.toString(occupied.getLocalPort()) }));
        }
    }

    @Test
    public void actualMainRegistersWithGrpcBootstrapAndRunsUntilStopped() throws Exception {
        WorkerRegistry registry = new WorkerRegistry(2);
        bootstrapServer = new BootstrapGrpcServer(0, registry);
        bootstrapServer.start();
        int bootstrapPort = bootstrapServer.getPort();

        int grpcPort, port;
        try (ServerSocket s1 = new ServerSocket(0); ServerSocket s2 = new ServerSocket(0)) {
            port = s1.getLocalPort();
            grpcPort = s2.getLocalPort();
        }

        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread main = new Thread(() -> WorkerMain.main(new String[] { "202", "localhost",
                Integer.toString(port), "localhost", Integer.toString(bootstrapPort),
                Integer.toString(grpcPort) }), "worker-main-test");
        main.setUncaughtExceptionHandler((thread, error) -> failure.set(error));

        try (var watch = registry.watchMembership()) {
            main.start();
            boolean registered = false;
            long deadline = System.currentTimeMillis() + 5000;
            while (System.currentTimeMillis() < deadline) {
                var update = watch.nextUpdate();
                if (update.isPresent() && update.get().hasChange()
                        && update.get().getChange().getWorker().getIdentity().getWorkerId() == 202) {
                    registered = true;
                    break;
                }
                Thread.sleep(50);
            }
            assertTrue(registered, () -> "Worker did not register with Bootstrap: " + failure.get());
            assertEquals(202, grpcStub(grpcPort).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
        } finally {
            main.interrupt();
            main.join(5_000);
        }
        assertFalse(main.isAlive());
        assertNull(failure.get());

        WorkerGrpcServer releasedGrpc = new WorkerGrpcServer(203, grpcPort, 1);
        try {
            releasedGrpc.start();
        } finally {
            releasedGrpc.stop();
        }
    }

    private WorkerNode worker(int id, int port) {
        WorkerNode worker = new WorkerNode(id, "localhost", port);
        workers.add(worker);
        return worker;
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub grpcStub(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        channels.add(channel);
        return WorkerServiceGrpc.newBlockingStub(channel);
    }
}

