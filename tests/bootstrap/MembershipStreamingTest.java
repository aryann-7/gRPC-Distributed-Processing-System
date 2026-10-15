package bootstrap;

import common.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MembershipStreamingTest {
    private WorkerRegistry registry;
    private BootstrapGrpcServer server;
    private ManagedChannel channel;
    private BootstrapServiceGrpc.BootstrapServiceBlockingStub stub;

    @BeforeEach
    public void setUp() throws Exception {
        registry = new WorkerRegistry();
        server = new BootstrapGrpcServer(0, registry);
        server.start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        stub = BootstrapServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            if (channel != null) {
                assertTrue(channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
            }
        } finally {
            if (server != null) {
                server.close();
            }
        }
        assertEquals(0, registry.membershipWatchCount());
    }

    @Test
    public void snapshotJoinsAndLeavesSkipRetriesRejectedOperationsAndDirectoryChanges() throws Exception {
        WorkerDescriptor worker = worker(101, "session");
        register(worker);
        try (var stream = subscribe(false)) {
            MembershipSnapshot snapshot = stream.next().getSnapshot();
            assertEquals(List.of(worker), snapshot.getWorkersList());
            assertEquals(1, snapshot.getMembershipRevision());
            assertEquals(registry.getEpoch(), snapshot.getRegistryEpoch());
            assertFalse(register(worker).getCreated());
            assertThrows(io.grpc.StatusRuntimeException.class, () -> register(worker(101, "conflict")));
            assertThrows(io.grpc.StatusRuntimeException.class,
                    () -> stub.deregisterWorker(DeregisterWorkerRequest.newBuilder()
                            .setWorker(worker(101, "stale").getIdentity()).setRegistryEpoch(registry.getEpoch()).build()));
            stub.publishCoordinator(PublishCoordinatorRequest.newBuilder().setRegistryEpoch(registry.getEpoch())
                    .setCoordinator(CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(1)).build());
            assertTrue(stub.deregisterWorker(DeregisterWorkerRequest.newBuilder().setWorker(worker.getIdentity())
                    .setRegistryEpoch(registry.getEpoch()).build()).getRemoved());
            assertChange(stream.next(), MembershipChange.Kind.LEFT, worker, 2);
            WorkerDescriptor joined = worker(102, "joined");
            register(joined);
            assertChange(stream.next(), MembershipChange.Kind.JOINED, joined, 3);
        }
    }

    @Test
    public void subscribersAreIndependentAndCancellationReleasesTheirWatchers() throws Exception {
        try (var first = subscribe(false); var second = subscribe(false)) {
            assertTrue(first.next().hasSnapshot());
            assertTrue(second.next().hasSnapshot());
            WorkerDescriptor worker = worker(101, "session");
            register(worker);
            assertChange(first.next(), MembershipChange.Kind.JOINED, worker, 1);
            assertChange(second.next(), MembershipChange.Kind.JOINED, worker, 1);
            first.close();
            assertEquals(Status.Code.CANCELLED, first.terminal.get(5, TimeUnit.SECONDS).getCode());
            awaitWatchCount(1);
            WorkerDescriptor next = worker(102, "next");
            register(next);
            assertChange(second.next(), MembershipChange.Kind.JOINED, next, 2);
            assertTrue(first.updates.isEmpty());
        }
        awaitWatchCount(0);
    }

    @Test
    public void concurrentJoinsAreEitherInSnapshotOrContiguousEvents() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        CountDownLatch start = new CountDownLatch(1);
        var joining = executor.submit(() -> {
            start.await();
            for (int id = 1; id <= 40; id++) {
                register(worker(id, "session-" + id));
            }
            return null;
        });
        try (var stream = subscribe(false)) {
            start.countDown();
            MembershipSnapshot snapshot = stream.next().getSnapshot();
            long revision = snapshot.getMembershipRevision();
            assertEquals(revision, snapshot.getWorkersCount());
            var workers = new java.util.HashSet<>(snapshot.getWorkersList());
            while (revision < 40) {
                MembershipChange change = stream.next().getChange();
                assertEquals(++revision, change.getMembershipRevision());
                assertEquals(registry.getEpoch(), change.getRegistryEpoch());
                assertEquals(MembershipChange.Kind.JOINED, change.getKind());
                assertTrue(workers.add(change.getWorker()));
            }
            joining.get(5, TimeUnit.SECONDS);
            assertEquals(new java.util.HashSet<>(registry.snapshot()), workers);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void forcedOverflowReturnsResourceExhaustedAndAllowsFreshSnapshot() throws Exception {
        try (var stream = subscribe(false)) {
            stream.next();
            // Hold the lock to make queue overflow deterministic before the reader can drain it.
            synchronized (registry) {
                for (int id = 1; id <= 300; id++) {
                    registry.registerWorker(worker(id, "session"));
                }
            }
            assertEquals(Status.Code.RESOURCE_EXHAUSTED, stream.terminal.get(5, TimeUnit.SECONDS).getCode());
            awaitWatchCount(0);
            try (var replacement = subscribe(false)) {
                MembershipSnapshot snapshot = replacement.next().getSnapshot();
                assertEquals(300, snapshot.getMembershipRevision());
                assertEquals(300, snapshot.getWorkersCount());
            }
        }
    }

    @Test
    public void serverShutdownEndsStreamsAndReleasesReaderThreads() throws Exception {
        try (var stream = subscribe(false)) {
            stream.next();
            server.close();
            assertEquals(Status.Code.UNAVAILABLE, stream.terminal.get(5, TimeUnit.SECONDS).getCode());
            assertEquals(0, registry.membershipWatchCount());
            assertTrue(Thread.getAllStackTraces().keySet().stream()
                    .noneMatch(thread -> thread.isAlive() && thread.getName().equals("bootstrap-membership")));
        }
    }

    @Test
    public void streamingCapacityIsBoundedAndUnaryCallsRemainAvailable() throws Exception {
        List<MembershipStream> streams = new ArrayList<>();
        try {
            for (int i = 0; i < 16; i++) {
                var stream = subscribe(false);
                streams.add(stream);
                stream.next();
            }
            try (var excess = subscribe(false)) {
                assertEquals(Status.Code.RESOURCE_EXHAUSTED, excess.terminal.get(5, TimeUnit.SECONDS).getCode());
            }
            assertEquals(16, registry.membershipWatchCount());
            assertTrue(register(worker(101, "session")).getCreated());
            for (var stream : streams) {
                assertEquals(1, stream.next().getChange().getMembershipRevision());
            }
        } finally {
            streams.forEach(MembershipStream::close);
        }
        awaitWatchCount(0);
    }

    @Test
    public void manualClientDemandResumesOrderedDeliveryUnderBackpressure() throws Exception {
        try (var stream = subscribe(true)) {
            assertTrue(stream.next().hasSnapshot());
            String session = "x".repeat(70000);
            for (int id = 1; id <= 30; id++) {
                register(worker(id, session));
            }
            assertNull(stream.updates.poll(100, TimeUnit.MILLISECONDS));
            stream.call.request(30);
            for (int id = 1; id <= 30; id++) {
                assertChange(stream.next(), MembershipChange.Kind.JOINED, worker(id, session), id);
            }
        }
    }

    private MembershipStream subscribe(boolean manualDemand) {
        MembershipStream stream = new MembershipStream(manualDemand);
        BootstrapServiceGrpc.newStub(channel).withDeadlineAfter(20, TimeUnit.SECONDS)
                .watchMembership(WatchMembershipRequest.getDefaultInstance(), stream);
        return stream;
    }

    private RegisterWorkerResponse register(WorkerDescriptor worker) {
        return stub.registerWorker(RegisterWorkerRequest.newBuilder().setWorker(worker).build());
    }

    private void awaitWatchCount(int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (registry.membershipWatchCount() != expected && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(expected, registry.membershipWatchCount());
    }

    private void assertChange(MembershipUpdate update, MembershipChange.Kind kind,
                              WorkerDescriptor worker, long revision) {
        assertTrue(update.hasChange());
        assertEquals(kind, update.getChange().getKind());
        assertEquals(worker, update.getChange().getWorker());
        assertEquals(revision, update.getChange().getMembershipRevision());
        assertEquals(registry.getEpoch(), update.getChange().getRegistryEpoch());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101)).build();
    }

    private static final class MembershipStream
            implements ClientResponseObserver<WatchMembershipRequest, MembershipUpdate>, AutoCloseable {
        private final ArrayBlockingQueue<MembershipUpdate> updates = new ArrayBlockingQueue<>(512);
        private final CompletableFuture<Status> terminal = new CompletableFuture<>();
        private final boolean manualDemand;
        private ClientCallStreamObserver<WatchMembershipRequest> call;

        private MembershipStream(boolean manualDemand) {
            this.manualDemand = manualDemand;
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<WatchMembershipRequest> call) {
            this.call = call;
            if (manualDemand) {
                call.disableAutoRequestWithInitial(1);
            }
        }

        @Override
        public void onNext(MembershipUpdate update) {
            if (!updates.offer(update)) {
                call.cancel("Test consumer queue overflow", null);
            }
        }

        @Override
        public void onError(Throwable error) {
            terminal.complete(Status.fromThrowable(error));
        }

        @Override
        public void onCompleted() {
            terminal.complete(Status.OK);
        }

        private MembershipUpdate next() throws Exception {
            MembershipUpdate update = updates.poll(5, TimeUnit.SECONDS);
            assertNotNull(update, "Timed out waiting for membership update");
            return update;
        }

        @Override
        public void close() {
            call.cancel("Test subscription closed", null);
        }
    }
}
