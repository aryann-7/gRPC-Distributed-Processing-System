package worker;

import bootstrap.BootstrapServiceImpl;
import common.interfaces.WorkerService;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/** Plain-Java integration checks using real local RMI calls; no worker processes. */
public class WorkerBootstrapIntegrationTest implements AutoCloseable {
    private final List<Remote> exported = new ArrayList<>();
    private final Map<Integer, Integer> workerPorts = new HashMap<>();
    private int bootstrapPort;

    public static void main(String[] args) throws Exception {
        try (WorkerBootstrapIntegrationTest test = new WorkerBootstrapIntegrationTest()) {
            test.testJoiningWorkers();
            test.testFailureOrdering();
        }
        System.out.println("All Worker-Bootstrap integration tests passed.");
    }

    private void testJoiningWorkers() throws Exception {
        RecordingBootstrap bootstrap = startBootstrap();
        WorkerNode a = worker(101);
        a.connectToBootstrap("localhost", bootstrapPort);
        check(bootstrap.discoveryFor(101).isEmpty(),
                "A must register successfully and receive an empty discovery result");
        check(neighbours(a).isEmpty(), "A must start without neighbours, including itself");
        System.out.println("PASS: first worker bootstrap");

        WorkerNode b = worker(102);
        b.connectToBootstrap("localhost", bootstrapPort);
        check(bootstrap.discoveryFor(102).equals(List.of(descriptor(a))),
                "B must receive A's exact workerId|host|port descriptor");
        check(neighbours(a).keySet().equals(Set.of(102)), "A must store only B");
        check(neighbours(b).keySet().equals(Set.of(101)), "B must store only A");
        checkStoredStub(a, b);
        checkStoredStub(b, a);
        System.out.println("PASS: bidirectional neighbour connection and discovery contract");

        WorkerService aStub = stub(a), bStub = stub(b);
        Map<Integer, WorkerService> original = neighbours(a);
        aStub.addNeighbour(102, bStub);
        aStub.addNeighbour(102, aStub);
        check(neighbours(a).equals(original),
                "Repeated neighbour IDs must preserve the original stub and map size");
        check(neighbours(b).keySet().equals(Set.of(101)),
                "Duplicate registration must not change the other worker's neighbours");
        System.out.println("PASS: duplicate neighbour registration");

        expectRejected(() -> aStub.addNeighbour(0, bStub), "zero neighbour ID");
        expectRejected(() -> aStub.addNeighbour(-1, bStub), "negative neighbour ID");
        expectRejected(() -> aStub.addNeighbour(101, aStub), "self neighbour ID");
        expectRejected(() -> aStub.addNeighbour(103, null), "null neighbour reference");
        check(neighbours(a).equals(original), "Rejected calls must preserve existing neighbours");
        System.out.println("PASS: invalid neighbour registration");

        WorkerNode c = worker(103);
        c.connectToBootstrap("localhost", bootstrapPort);
        List<String> selectedDescriptors = bootstrap.discoveryFor(103);
        check(selectedDescriptors.size() >= 1 && selectedDescriptors.size() <= 2,
                "C must receive up to 2 initial peers");
        for (String desc : selectedDescriptors) {
            check(Set.of(descriptor(a), descriptor(b)).contains(desc),
                    "C's descriptor must identify an eligible existing worker");
        }
        for (String desc : selectedDescriptors) {
            WorkerNode peer = desc.equals(descriptor(a)) ? a : b;
            check(neighbours(c).containsKey(peer.getWorkerId()),
                    "C must store peer connected from Bootstrap");
            check(neighbours(peer).containsKey(103),
                    "Peer must store C as bidirectional neighbour");
            checkStoredStub(c, peer);
            checkStoredStub(peer, c);
        }
        for (WorkerNode worker : List.of(a, b, c)) {
            check(!neighbours(worker).containsKey(worker.getWorkerId()),
                    "No worker may contain itself as a neighbour");
        }
        System.out.println("PASS: three-worker subset topology");
    }

    private void testFailureOrdering() throws Exception {
        RecordingBootstrap bootstrap = startBootstrap();
        int peerPort = registry();
        RejectingWorker peer = track(new RejectingWorker(201, peerPort));
        bind(peer, peerPort);
        peer.connectToBootstrap("localhost", bootstrapPort);
        check(bootstrap.discoveryFor(201).isEmpty(), "Rejecting peer must register first");

        WorkerNode joining = worker(202);
        peer.joiningWorker = joining;
        joining.connectToBootstrap("localhost", bootstrapPort);
        check(bootstrap.discoveryFor(202).equals(List.of(descriptor(peer))),
                "The joining worker must discover the rejecting peer through Bootstrap");
        check(peer.registrationCalls == 1, "Reverse registration must be attempted exactly once");
        check(peer.localMapWasEmpty, "The local map must be empty during reverse registration");
        check(peer.receivedId == 202 && stub(joining).equals(peer.receivedStub),
                "Reverse registration must receive the joining worker's ID and exported stub");
        check(neighbours(joining).isEmpty(),
                "Failed reverse registration must not leave the selected peer stored locally");
        System.out.println("PASS: reverse registration failure ordering");

        stub(joining).addNeighbour(201, stub(peer));
        check(peer.registrationCalls == 1,
                "Receiving addNeighbour must store the supplied stub without calling it back");
        check(neighbours(joining).keySet().equals(Set.of(201)),
                "Direct registration must store the neighbour without a registration cycle");
        System.out.println("PASS: neighbour registration does not call back");
    }

    private void checkStoredStub(WorkerNode owner, WorkerNode expected) throws Exception {
        WorkerService stored = neighbours(owner).get(expected.getWorkerId());
        check(stored != null && stored != expected && stored.equals(stub(expected)),
                "Stored reference must match the Worker-<id> registry stub");
        check(stored.ping(), "Stored WorkerService stub must be callable over RMI");
    }

    // Reflection is confined to test observations and resource cleanup, not production APIs.
    @SuppressWarnings("unchecked")
    private static Map<Integer, WorkerService> neighbours(WorkerNode node) throws Exception {
        Field field = WorkerNode.class.getDeclaredField("neighbours");
        field.setAccessible(true);
        return new HashMap<>((Map<Integer, WorkerService>) field.get(node));
    }

    private static void stopExecutor(WorkerNode node) throws Exception {
        Field field = WorkerNode.class.getDeclaredField("computationThreadPool");
        field.setAccessible(true);
        ((ExecutorService) field.get(node)).shutdownNow();
    }

    private static void expectRejected(RemoteCall call, String description) throws Exception {
        try {
            call.run();
        } catch (RemoteException expected) {
            return;
        }
        throw new AssertionError("Expected RemoteException for " + description);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    @FunctionalInterface
    private interface RemoteCall {
        void run() throws Exception;
    }

    /** Records the actual random result without changing production discovery behaviour. */
    private static class RecordingBootstrap extends BootstrapServiceImpl {
        private final Map<Integer, List<String>> discoveries = new ConcurrentHashMap<>();

        RecordingBootstrap() throws RemoteException {
            super();
        }

        @Override
        public List<String> getInitialNeighbors(int workerId) throws RemoteException {
            List<String> result = super.getInitialNeighbors(workerId);
            discoveries.put(workerId, List.copyOf(result));
            return result;
        }

        List<String> discoveryFor(int workerId) {
            List<String> result = discoveries.get(workerId);
            check(result != null, "Worker " + workerId + " must successfully register and request discovery");
            return result;
        }
    }

    /** Only reverse registration fails; construction, binding and Bootstrap joining are unchanged. */
    private static class RejectingWorker extends WorkerNode {
        private volatile WorkerNode joiningWorker;
        private volatile int registrationCalls;
        private volatile boolean localMapWasEmpty;
        private volatile int receivedId;
        private volatile WorkerService receivedStub;

        RejectingWorker(int id, int port) throws RemoteException {
            super(id, "localhost", port);
        }

        @Override
        public void addNeighbour(int neighbourId, WorkerService neighbour) throws RemoteException {
            registrationCalls++;
            receivedId = neighbourId;
            receivedStub = neighbour;
            try {
                localMapWasEmpty = neighbours(joiningWorker).isEmpty();
            } catch (Exception e) {
                throw new RemoteException("Could not inspect joining worker", e);
            }
            throw new RemoteException("Intentional test rejection of reverse registration");
        }
    }

    private <T extends Remote> T track(T object) {
        exported.add(object);
        return object;
    }

    private int registry() throws RemoteException {
        // Bind directly to an OS-assigned port; do not release it before RMI uses it.
        int[] port = new int[1];
        track(LocateRegistry.createRegistry(0, null, requestedPort -> {
            ServerSocket socket = new ServerSocket(requestedPort);
            port[0] = socket.getLocalPort();
            return socket;
        }));
        return port[0];
    }

    private RecordingBootstrap startBootstrap() throws RemoteException {
        bootstrapPort = registry();
        RecordingBootstrap service = track(new RecordingBootstrap());
        LocateRegistry.getRegistry("localhost", bootstrapPort).rebind("BootstrapService", service);
        return service;
    }

    private WorkerNode worker(int id) throws Exception {
        int port = registry();
        WorkerNode node = track(new WorkerNode(id, "localhost", port));
        bind(node, port);
        return node;
    }

    private void bind(WorkerNode node, int port) throws Exception {
        LocateRegistry.getRegistry("localhost", port).rebind("Worker-" + node.getWorkerId(), node);
        workerPorts.put(node.getWorkerId(), port);
        check(stub(node) != node, "Registry lookup must return a remote stub");
    }

    private WorkerService stub(WorkerNode node) throws Exception {
        Registry registry = LocateRegistry.getRegistry("localhost", workerPorts.get(node.getWorkerId()));
        return (WorkerService) registry.lookup("Worker-" + node.getWorkerId());
    }

    private String descriptor(WorkerNode node) {
        return node.getWorkerId() + "|localhost|" + workerPorts.get(node.getWorkerId());
    }

    @Override
    public void close() throws Exception {
        Exception failure = null;
        for (int i = exported.size() - 1; i >= 0; i--) {
            Remote object = exported.get(i);
            try {
                try {
                    if (object instanceof WorkerNode) {
                        stopExecutor((WorkerNode) object);
                    }
                } finally {
                    UnicastRemoteObject.unexportObject(object, true);
                }
            } catch (Exception e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        if (failure != null) {
            throw failure;
        }
    }
}
