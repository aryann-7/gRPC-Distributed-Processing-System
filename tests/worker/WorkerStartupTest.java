package worker;

import bootstrap.BootstrapServiceImpl;
import common.grpc.HeartbeatRequest;
import common.grpc.WorkerServiceGrpc;
import common.interfaces.WorkerService;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import worker.grpc.WorkerGrpcServer;

import java.net.ServerSocket;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerStartupTest {
    private final List<WorkerNode> workers = new ArrayList<>();
    private final List<Registry> registries = new ArrayList<>();
    private final List<ManagedChannel> channels = new ArrayList<>();

    @AfterEach
    public void tearDown() throws Exception {
        for (ManagedChannel channel : channels) {
            assertTrue(channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        }
        workers.forEach(WorkerNode::stop);
        for (Registry registry : registries) {
            try {
                UnicastRemoteObject.unexportObject(registry, true);
            } catch (NoSuchObjectException ignored) {
            }
        }
    }

    @Test
    public void rmiAndGrpcServeTheSameWorkerOnDistinctPortsAndStopCleanly() throws Exception {
        RegistryAddress address = registry();
        WorkerNode worker = worker(201, address.port());
        address.registry().rebind("Worker-201", worker);
        WorkerService remote = (WorkerService) LocateRegistry.getRegistry("localhost", address.port()).lookup("Worker-201");
        assertTrue(remote.ping());
        int grpcPort = worker.getGrpcServer().getPort();
        assertTrue(grpcPort > 0);
        assertNotEquals(address.port(), grpcPort);
        var stub = grpcStub(grpcPort);
        assertEquals(201, stub.withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
        worker.stop();
        worker.stop();
        assertThrows(RemoteException.class, remote::ping);
        WorkerGrpcServer replacement = new WorkerGrpcServer(202, grpcPort, 1);
        try {
            replacement.start();
        } finally {
            replacement.stop();
        }
    }

    @Test
    public void multipleWorkersReceiveDistinctGrpcPorts() throws Exception {
        RegistryAddress firstRmi = registry(), secondRmi = registry();
        WorkerNode first = worker(201, firstRmi.port()), second = worker(202, secondRmi.port());
        assertNotEquals(first.getGrpcServer().getPort(), second.getGrpcServer().getPort());
        assertEquals(201, grpcStub(first.getGrpcServer().getPort()).withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
        assertEquals(202, grpcStub(second.getGrpcServer().getPort()).withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
    }

    @Test
    public void occupiedOrIdenticalGrpcPortFailsConstructionClearly() throws Exception {
        RegistryAddress rmi = registry();
        try (ServerSocket occupied = new ServerSocket(0)) {
            RemoteException error = assertThrows(RemoteException.class,
                    () -> new WorkerNode(201, "localhost", rmi.port(), occupied.getLocalPort()));
            assertTrue(error.getMessage().contains("gRPC startup failed"));
            assertNotNull(error.getCause());
        }
        assertThrows(RemoteException.class, () -> new WorkerNode(201, "localhost", rmi.port(), rmi.port()));
        assertEquals(0, LocateRegistry.getRegistry("localhost", rmi.port()).list().length);
    }

    @Test
    public void mainStartupFailureReleasesItsRmiRegistry() throws Exception {
        int rmiPort;
        try (ServerSocket reservation = new ServerSocket(0)) {
            rmiPort = reservation.getLocalPort();
        }
        try (ServerSocket occupied = new ServerSocket(0)) {
            assertThrows(IllegalStateException.class, () -> WorkerMain.main(new String[] {
                    "201", "localhost", Integer.toString(rmiPort), "localhost", "1099",
                    Integer.toString(occupied.getLocalPort()) }));
            Registry replacement = LocateRegistry.createRegistry(rmiPort);
            registries.add(replacement);
            assertEquals(0, replacement.list().length);
        }
    }

    @Test
    public void actualMainRegistersWithRmiBootstrapAndRunsGrpcUntilStopped() throws Exception {
        RegistryAddress bootstrap = registry();
        int grpcPort, rmiPort;
        try (ServerSocket rmi = new ServerSocket(0); ServerSocket grpc = new ServerSocket(0)) {
            rmiPort = rmi.getLocalPort();
            grpcPort = grpc.getLocalPort();
        }
        CountDownLatch registered = new CountDownLatch(1);
        AtomicReference<String> advertised = new AtomicReference<>();
        BootstrapServiceImpl service = new BootstrapServiceImpl() {
            @Override
            public void registerWorker(int id, String host, int port) throws RemoteException {
                super.registerWorker(id, host, port);
                advertised.set(id + "|" + host + "|" + port);
                registered.countDown();
            }
        };
        bootstrap.registry().rebind("BootstrapService", service);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread main = new Thread(() -> WorkerMain.main(new String[] { "202", "localhost",
                Integer.toString(rmiPort), "localhost", Integer.toString(bootstrap.port()),
                Integer.toString(grpcPort) }), "worker-main-test");
        main.setUncaughtExceptionHandler((thread, error) -> failure.set(error));
        try {
            main.start();
            assertTrue(registered.await(5, TimeUnit.SECONDS), () -> "Worker did not register with Bootstrap: " + failure.get());
            assertEquals("202|localhost|" + rmiPort, advertised.get());
            WorkerService remote = (WorkerService) LocateRegistry.getRegistry("localhost", rmiPort)
                    .lookup("Worker-202");
            assertTrue(remote.ping());
            assertEquals(202, grpcStub(grpcPort).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId());
        } finally {
            main.interrupt();
            main.join(5_000);
            UnicastRemoteObject.unexportObject(service, true);
        }
        assertFalse(main.isAlive());
        assertNull(failure.get());
        Registry releasedRmi = LocateRegistry.createRegistry(rmiPort);
        registries.add(releasedRmi);
        WorkerGrpcServer releasedGrpc = new WorkerGrpcServer(203, grpcPort, 1);
        try {
            releasedGrpc.start();
        } finally {
            releasedGrpc.stop();
        }
    }

    private WorkerNode worker(int id, int rmiPort) throws RemoteException {
        WorkerNode worker = new WorkerNode(id, "localhost", rmiPort);
        workers.add(worker);
        return worker;
    }

    private WorkerServiceGrpc.WorkerServiceBlockingStub grpcStub(int port) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
        channels.add(channel);
        return WorkerServiceGrpc.newBlockingStub(channel);
    }

    private RegistryAddress registry() throws RemoteException {
        AtomicInteger port = new AtomicInteger();
        Registry registry = LocateRegistry.createRegistry(0, null, requested -> {
            ServerSocket socket = new ServerSocket(requested);
            port.set(socket.getLocalPort());
            return socket;
        });
        registries.add(registry);
        return new RegistryAddress(registry, port.get());
    }

    private record RegistryAddress(Registry registry, int port) {}
}
