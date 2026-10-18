package bootstrap;

import common.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class LeaseGrpcTest {
    private final AtomicLong nanos = new AtomicLong();
    private final AtomicLong wallMillis = new AtomicLong(1_700_000_000_000L);
    private final WorkerRegistry registry = new WorkerRegistry(2, nanos::get);
    private BootstrapGrpcServer server;
    private ManagedChannel channel;
    private BootstrapServiceGrpc.BootstrapServiceBlockingStub stub;

    private void start(long intervalMillis) throws Exception {
        server = new BootstrapGrpcServer(0, registry, 100, intervalMillis, wallMillis::get);
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
    public void activationRenewalAndRegistrationRetryReturnConsistentMetadataWithoutEvents() throws Exception {
        start(60_000);
        WorkerDescriptor worker = worker(101, "session");
        RegisterWorkerResponse registered = register(worker);
        assertEquals(0, registered.getLeaseDurationMs());
        assertEquals(0, registered.getLeaseExpiresAtMs());
        try (var stream = subscribe()) {
            stream.next();
            RenewLeaseResponse first = renew(worker.getIdentity());
            assertEquals(worker.getIdentity(), first.getWorker());
            assertEquals(100, first.getLeaseDurationMs());
            assertEquals(wallMillis.get() + 100, first.getLeaseExpiresAtMs());
            assertEquals(registry.getEpoch(), first.getRegistryEpoch());
            assertEquals(1, first.getMembershipRevision());
            advanceMillis(90);
            RenewLeaseResponse renewed = renew(worker.getIdentity());
            assertEquals(wallMillis.get() + 100, renewed.getLeaseExpiresAtMs());
            advanceMillis(90);
            RegisterWorkerResponse retry = register(worker);
            assertFalse(retry.getCreated());
            assertEquals(100, retry.getLeaseDurationMs());
            assertEquals(wallMillis.get() + 100, retry.getLeaseExpiresAtMs());
            assertEquals(1, retry.getMembershipRevision());
            advanceMillis(90);
            assertEquals(0, registry.expireLeases());
            WorkerDescriptor next = worker(102, "next");
            register(next);
            MembershipChange change = stream.next().getChange();
            assertEquals(MembershipChange.Kind.JOINED, change.getKind());
            assertEquals(next, change.getWorker());
            assertEquals(2, change.getMembershipRevision());
        }
    }

    @Test
    public void renewalRejectsMissingInvalidUnknownStaleAndWrongEpochRequests() throws Exception {
        start(60_000);
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.renewLease(RenewLeaseRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> renew(WorkerIdentity.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> renew(worker(0, "session").getIdentity()));
        WorkerDescriptor worker = worker(101, "current");
        assertStatus(Status.Code.NOT_FOUND, () -> renew(worker.getIdentity()));
        register(worker);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> renew(worker(101, "stale").getIdentity()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.renewLease(RenewLeaseRequest.newBuilder()
                .setWorker(worker.getIdentity()).setRegistryEpoch("old-epoch").build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.renewLease(RenewLeaseRequest.newBuilder()
                .setWorker(worker.getIdentity()).build()));
        advanceMillis(1_000);
        assertEquals(0, registry.expireLeases());
        assertEquals(worker, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void overdueLeaseCannotBeRevivedBeforeOrAfterRemoval() throws Exception {
        start(60_000);
        WorkerDescriptor old = worker(101, "old");
        register(old);
        renew(old.getIdentity());
        advanceMillis(100);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> renew(old.getIdentity()));
        assertEquals(1, registry.expireLeases());
        assertStatus(Status.Code.NOT_FOUND, () -> renew(old.getIdentity()));
        WorkerDescriptor replacement = worker(101, "new");
        register(replacement);
        renew(replacement.getIdentity());
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> renew(old.getIdentity()));
        assertEquals(replacement, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void automaticExpiryStreamsOneLeaveAndClearsCoordinatorWhilePreservingOtherWorkers() throws Exception {
        start(10);
        WorkerDescriptor coordinator = worker(101, "coordinator"), legacy = worker(102, "legacy"), live = worker(103, "live");
        register(coordinator);
        register(legacy);
        register(live);
        stub.publishCoordinator(PublishCoordinatorRequest.newBuilder().setRegistryEpoch(registry.getEpoch())
                .setCoordinator(CoordinatorRecord.newBuilder().setCoordinator(coordinator).setTerm(3)).build());
        renew(coordinator.getIdentity());
        advanceMillis(50);
        renew(live.getIdentity());
        try (var stream = subscribe()) {
            assertEquals(3, stream.next().getSnapshot().getMembershipRevision());
            advanceMillis(50);
            MembershipChange left = stream.next().getChange();
            assertEquals(MembershipChange.Kind.LEFT, left.getKind());
            assertEquals(coordinator, left.getWorker());
            assertEquals(4, left.getMembershipRevision());
            assertEquals(registry.getEpoch(), left.getRegistryEpoch());
            assertFalse(stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance()).hasCoordinator());
            var snapshot = stub.listWorkers(ListWorkersRequest.getDefaultInstance()).getSnapshot();
            assertEquals(2, snapshot.getWorkersCount());
            assertTrue(snapshot.getWorkersList().contains(legacy));
            assertTrue(snapshot.getWorkersList().contains(live));
            var neighbours = stub.getInitialNeighbours(GetInitialNeighboursRequest.newBuilder()
                    .setRequester(legacy.getIdentity()).setRegistryEpoch(registry.getEpoch()).build());
            assertEquals(java.util.List.of(live), neighbours.getNeighboursList());
            WorkerDescriptor replacement = worker(101, "new");
            register(replacement);
            assertEquals(5, stream.next().getChange().getMembershipRevision());
            assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.publishCoordinator(PublishCoordinatorRequest.newBuilder()
                    .setRegistryEpoch(registry.getEpoch()).setCoordinator(CoordinatorRecord.newBuilder()
                            .setCoordinator(replacement).setTerm(3)).build()));
        }
    }

    @Test
    public void invalidWallClockCannotActivateALeaseOrReturnAnOverflowedTimestamp() throws Exception {
        start(60_000);
        WorkerDescriptor worker = worker(101, "session");
        register(worker);
        for (long invalid : new long[] { -1, Long.MAX_VALUE - 50 }) {
            wallMillis.set(invalid);
            assertStatus(Status.Code.INTERNAL, () -> renew(worker.getIdentity()));
            advanceMillis(1_000);
            assertEquals(0, registry.expireLeases());
        }
        wallMillis.set(1_700_000_000_000L);
        assertEquals(wallMillis.get() + 100, renew(worker.getIdentity()).getLeaseExpiresAtMs());
    }

    @Test
    public void shutdownStopsTheExpiryScheduler() throws Exception {
        start(10);
        WorkerDescriptor worker = worker(101, "session");
        register(worker);
        renew(worker.getIdentity());
        server.close();
        assertTrue(Thread.getAllStackTraces().keySet().stream()
                .noneMatch(thread -> thread.isAlive() && thread.getName().equals("bootstrap-lease-expiry")));
        advanceMillis(1_000);
        assertEquals(worker, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void invalidLeaseConfigurationIsRejectedBeforeStartingThreads() {
        for (long duration : new long[] { 0, -1, Long.MAX_VALUE }) {
            assertThrows(IllegalArgumentException.class, () -> new BootstrapGrpcServer(0, registry, duration, 10));
        }
        for (long interval : new long[] { 0, -1 }) {
            assertThrows(IllegalArgumentException.class, () -> new BootstrapGrpcServer(0, registry, 100, interval));
        }
    }

    private RegisterWorkerResponse register(WorkerDescriptor worker) {
        return stub.registerWorker(RegisterWorkerRequest.newBuilder().setWorker(worker).build());
    }

    private RenewLeaseResponse renew(WorkerIdentity worker) {
        return stub.renewLease(RenewLeaseRequest.newBuilder().setWorker(worker).setRegistryEpoch(registry.getEpoch()).build());
    }

    private void advanceMillis(long millis) {
        nanos.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
        wallMillis.addAndGet(millis);
    }

    private static void assertStatus(Status.Code code, Runnable call) {
        assertEquals(code, assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101)).build();
    }

    private LeaseStream subscribe() {
        LeaseStream stream = new LeaseStream();
        BootstrapServiceGrpc.newStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS)
                .watchMembership(WatchMembershipRequest.getDefaultInstance(), stream);
        return stream;
    }

    private static final class LeaseStream
            implements ClientResponseObserver<WatchMembershipRequest, MembershipUpdate>, AutoCloseable {
        private final ArrayBlockingQueue<Object> updates = new ArrayBlockingQueue<>(16);
        private ClientCallStreamObserver<WatchMembershipRequest> call;

        @Override
        public void beforeStart(ClientCallStreamObserver<WatchMembershipRequest> call) {
            this.call = call;
        }

        @Override
        public void onNext(MembershipUpdate update) {
            assertTrue(updates.offer(update));
        }

        @Override
        public void onError(Throwable error) {
            updates.offer(error);
        }

        @Override
        public void onCompleted() {}

        private MembershipUpdate next() throws Exception {
            Object update = updates.poll(5, TimeUnit.SECONDS);
            assertInstanceOf(MembershipUpdate.class, update);
            return (MembershipUpdate) update;
        }

        @Override
        public void close() {
            call.cancel("Test stream closed", null);
        }
    }
}
