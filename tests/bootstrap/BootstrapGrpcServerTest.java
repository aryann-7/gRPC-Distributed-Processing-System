package bootstrap;

import common.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class BootstrapGrpcServerTest {
    private WorkerRegistry registry;
    private BootstrapGrpcServer server;
    private ManagedChannel channel;
    private BootstrapServiceGrpc.BootstrapServiceBlockingStub stub;

    @BeforeEach
    public void setUp() throws Exception {
        registry = new WorkerRegistry(2);
        server = new BootstrapGrpcServer(0, registry);
        server.start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        stub = BootstrapServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            if (channel != null) {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } finally {
            if (server != null) {
                server.close();
            }
        }
    }

    @Test
    public void registrationRetriesAndListingShareRegistryState() {
        WorkerDescriptor worker = worker(101, "session-1");
        RegisterWorkerResponse first = register(worker);
        assertTrue(first.getCreated());
        assertEquals(1, first.getMembershipRevision());
        assertEquals(registry.getEpoch(), first.getRegistryEpoch());
        assertFalse(register(worker).getCreated());
        MembershipSnapshot listed = stub.listWorkers(ListWorkersRequest.getDefaultInstance()).getSnapshot();
        assertEquals(1, listed.getMembershipRevision());
        assertEquals(worker, listed.getWorkers(0));
        assertEquals(registry.snapshot(), listed.getWorkersList());
    }

    @Test
    public void conflictingRegistrationDoesNotReplaceWorker() {
        WorkerDescriptor worker = worker(101, "session-1");
        register(worker);
        assertStatus(Status.Code.ALREADY_EXISTS, () -> register(worker(101, "session-2")));
        assertStatus(Status.Code.ALREADY_EXISTS, () -> register(worker.toBuilder()
                .setEndpoint(worker.getEndpoint().toBuilder().setPort(1201)).build()));
        assertEquals(worker, registry.findWorker(101).orElseThrow());
        assertEquals(1, registry.membershipSnapshot().getMembershipRevision());
    }

    @Test
    public void neighboursExcludeSelfDuplicatesAndRemovedWorkers() {
        WorkerDescriptor requester = worker(101, "session-101");
        register(requester);
        assertTrue(neighbours(requester.getIdentity()).getNeighboursList().isEmpty());
        for (int id = 102; id <= 105; id++) {
            register(worker(id, "session-" + id));
        }
        deregister(worker(105, "session-105").getIdentity());
        GetInitialNeighboursResponse response = neighbours(requester.getIdentity());
        assertEquals(2, response.getNeighboursCount());
        assertEquals(2, response.getNeighboursList().stream().distinct().count());
        assertTrue(response.getNeighboursList().stream().allMatch(w -> w.getIdentity().getWorkerId() >= 102
                && w.getIdentity().getWorkerId() <= 104));
        assertEquals(6, response.getMembershipRevision());
    }

    @Test
    public void deregistrationIsIdempotentAndOldSessionCannotRemoveReplacement() {
        WorkerDescriptor old = worker(101, "old-session");
        register(old);
        assertTrue(deregister(old.getIdentity()).getRemoved());
        assertFalse(deregister(old.getIdentity()).getRemoved());
        WorkerDescriptor replacement = worker(101, "new-session");
        register(replacement);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> deregister(old.getIdentity()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> neighbours(old.getIdentity()));
        assertEquals(replacement, registry.findWorker(101).orElseThrow());
        assertEquals(3, registry.membershipSnapshot().getMembershipRevision());
    }

    @Test
    public void invalidRequestsAndUnknownWorkersReturnAppropriateStatuses() {
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> stub.registerWorker(RegisterWorkerRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> register(worker(0, "session")));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> register(worker(101, "session").toBuilder()
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(0)).build()));
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> stub.getInitialNeighbours(GetInitialNeighboursRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> stub.deregisterWorker(DeregisterWorkerRequest.getDefaultInstance()));
        assertStatus(Status.Code.NOT_FOUND, () -> neighbours(worker(999, "unknown").getIdentity()));
        assertTrue(stub.listWorkers(ListWorkersRequest.getDefaultInstance()).getSnapshot().getWorkersList().isEmpty());
    }

    @Test
    public void staleEpochCannotChangeMembership() {
        WorkerDescriptor worker = worker(101, "session");
        register(worker);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.deregisterWorker(DeregisterWorkerRequest.newBuilder()
                .setWorker(worker.getIdentity()).setRegistryEpoch("old-epoch").build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.getInitialNeighbours(GetInitialNeighboursRequest.newBuilder()
                .setRequester(worker.getIdentity()).setRegistryEpoch("old-epoch").build()));
        assertEquals(worker, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void deferredMethodsRemainUnimplemented() {
        assertStatus(Status.Code.UNIMPLEMENTED, () -> stub.renewLease(RenewLeaseRequest.getDefaultInstance()));
        assertStatus(Status.Code.UNIMPLEMENTED, () -> stub.watchMembership(WatchMembershipRequest.getDefaultInstance()).hasNext());
    }

    @Test
    public void shutdownReleasesListeningPort() throws Exception {
        int port = server.getPort();
        server.close();
        try (BootstrapGrpcServer replacement = new BootstrapGrpcServer(port)) {
            replacement.start();
            assertEquals(port, replacement.getPort());
        }
        assertTrue(channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    public void discoveryWithoutCoordinatorReturnsAnAbsentRecordAndEpoch() {
        GetCoordinatorResponse response = stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance());
        assertFalse(response.hasCoordinator());
        assertEquals(registry.getEpoch(), response.getRegistryEpoch());
        assertFalse(clear(worker(101, "session").getIdentity(), 1).getCleared());
    }

    @Test
    public void publicationDiscoveryAndRetryPreserveTheRegisteredDescriptor() {
        WorkerDescriptor worker = worker(101, "session");
        RegisterWorkerResponse registration = register(worker);
        CoordinatorRecord record = CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(2).build();
        PublishCoordinatorResponse published = publish(record);
        assertTrue(published.getUpdated());
        assertEquals(record, published.getCoordinator());
        assertEquals(registration.getRegistryEpoch(), published.getRegistryEpoch());
        GetCoordinatorResponse discovered = stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance());
        assertEquals(record, discovered.getCoordinator());
        assertEquals(registration.getRegistryEpoch(), discovered.getRegistryEpoch());
        assertFalse(publish(record).getUpdated());
        assertEquals(record, stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance()).getCoordinator());
        assertEquals(1, stub.listWorkers(ListWorkersRequest.getDefaultInstance()).getSnapshot().getMembershipRevision());
    }

    @Test
    public void invalidCoordinatorRequestsAndStaleRegistrationsAreRejected() {
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> stub.publishCoordinator(PublishCoordinatorRequest.getDefaultInstance()));
        assertStatus(Status.Code.INVALID_ARGUMENT,
                () -> stub.clearCoordinator(ClearCoordinatorRequest.getDefaultInstance()));
        WorkerDescriptor worker = worker(101, "current");
        CoordinatorRecord record = CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(1).build();
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(record));
        register(worker);
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> publish(record.toBuilder().setTerm(0).build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(record.toBuilder()
                .setCoordinator(worker(101, "old")).build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(record.toBuilder()
                .setCoordinator(worker.toBuilder().setEndpoint(worker.getEndpoint().toBuilder().setPort(1201))).build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.publishCoordinator(PublishCoordinatorRequest.newBuilder()
                .setCoordinator(record).setRegistryEpoch("old-epoch").build()));
        assertFalse(stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance()).hasCoordinator());
    }

    @Test
    public void newerElectionReplacesCoordinatorAndRejectsStaleClearing() {
        WorkerDescriptor first = worker(101, "first"), second = worker(102, "second");
        register(first);
        register(second);
        CoordinatorRecord old = CoordinatorRecord.newBuilder().setCoordinator(first).setTerm(2).build();
        CoordinatorRecord current = CoordinatorRecord.newBuilder().setCoordinator(second).setTerm(3).build();
        publish(old);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(current.toBuilder().setTerm(1).build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(current.toBuilder().setTerm(2).build()));
        assertTrue(publish(current).getUpdated());
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> clear(first.getIdentity(), 2));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> clear(worker(102, "old-session").getIdentity(), 3));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> clear(second.getIdentity(), 2));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> stub.clearCoordinator(ClearCoordinatorRequest.newBuilder()
                .setCoordinator(second.getIdentity()).setTerm(3).setRegistryEpoch("old-epoch").build()));
        assertEquals(current, stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance()).getCoordinator());
    }

    @Test
    public void clearingIsIdempotentAndRetainsTheTermWatermark() {
        WorkerDescriptor worker = worker(101, "session");
        register(worker);
        CoordinatorRecord record = CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(3).build();
        publish(record);
        ClearCoordinatorResponse cleared = clear(worker.getIdentity(), 3);
        assertTrue(cleared.getCleared());
        assertEquals(registry.getEpoch(), cleared.getRegistryEpoch());
        assertFalse(clear(worker.getIdentity(), 3).getCleared());
        assertFalse(stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance()).hasCoordinator());
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(record));
        assertTrue(publish(record.toBuilder().setTerm(4).build()).getUpdated());
        assertEquals(1, stub.listWorkers(ListWorkersRequest.getDefaultInstance()).getSnapshot().getMembershipRevision());
    }

    @Test
    public void deregistrationRemovesDiscoveryAndFencesThePreviousSession() {
        WorkerDescriptor worker = worker(101, "old");
        register(worker);
        CoordinatorRecord record = CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(3).build();
        publish(record);
        assertTrue(deregister(worker.getIdentity()).getRemoved());
        assertFalse(stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance()).hasCoordinator());
        WorkerDescriptor replacement = worker(101, "new");
        register(replacement);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(record.toBuilder().setTerm(4).build()));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> publish(record.toBuilder().setCoordinator(replacement).build()));
        assertTrue(publish(record.toBuilder().setCoordinator(replacement).setTerm(4).build()).getUpdated());
        assertEquals(replacement, stub.getCoordinator(GetCoordinatorRequest.getDefaultInstance())
                .getCoordinator().getCoordinator());
    }

    private PublishCoordinatorResponse publish(CoordinatorRecord coordinator) {
        return stub.publishCoordinator(PublishCoordinatorRequest.newBuilder().setCoordinator(coordinator)
                .setRegistryEpoch(registry.getEpoch()).build());
    }

    private ClearCoordinatorResponse clear(WorkerIdentity coordinator, int term) {
        return stub.clearCoordinator(ClearCoordinatorRequest.newBuilder().setCoordinator(coordinator)
                .setTerm(term).setRegistryEpoch(registry.getEpoch()).build());
    }

    private RegisterWorkerResponse register(WorkerDescriptor worker) {
        return stub.registerWorker(RegisterWorkerRequest.newBuilder().setWorker(worker).build());
    }

    private GetInitialNeighboursResponse neighbours(WorkerIdentity worker) {
        return stub.getInitialNeighbours(GetInitialNeighboursRequest.newBuilder().setRequester(worker)
                .setRegistryEpoch(registry.getEpoch()).build());
    }

    private DeregisterWorkerResponse deregister(WorkerIdentity worker) {
        return stub.deregisterWorker(DeregisterWorkerRequest.newBuilder().setWorker(worker)
                .setRegistryEpoch(registry.getEpoch()).build());
    }

    private static void assertStatus(Status.Code expected, Runnable call) {
        assertEquals(expected, assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101)).build();
    }
}
