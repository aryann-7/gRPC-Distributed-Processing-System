package worker.grpc;

import bootstrap.BootstrapGrpcServer;
import bootstrap.WorkerRegistry;
import common.grpc.BootstrapServiceGrpc;
import common.grpc.RegisterWorkerRequest;
import common.grpc.RegisterWorkerResponse;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class BootstrapGrpcClientTest {
    private WorkerRegistry registry;
    private BootstrapGrpcServer server;
    private BootstrapGrpcClient client;

    @BeforeEach
    public void setUp() throws Exception {
        registry = new WorkerRegistry(2);
        server = new BootstrapGrpcServer(0, registry);
        server.start();
        client = new BootstrapGrpcClient("127.0.0.1", server.getPort());
    }

    @AfterEach
    public void tearDown() {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (server != null) {
                server.close();
            }
        }
    }

    @Test
    public void registrationAndIdenticalRetryReturnIdentityAndEpoch() {
        WorkerDescriptor worker = worker(101, "session");
        RegisterWorkerResponse first = client.registerWorker(worker);
        assertTrue(first.getCreated());
        assertEquals(worker, first.getWorker());
        assertEquals(registry.getEpoch(), first.getRegistryEpoch());
        assertEquals(1, first.getMembershipRevision());
        RegisterWorkerResponse retry = client.registerWorker(worker);
        assertFalse(retry.getCreated());
        assertEquals(first.getWorker(), retry.getWorker());
        assertEquals(first.getRegistryEpoch(), retry.getRegistryEpoch());
        assertEquals(first.getMembershipRevision(), retry.getMembershipRevision());
        assertEquals(List.of(worker), registry.snapshot());
    }

    @Test
    public void conflictingSessionOrEndpointPropagatesAlreadyExists() {
        WorkerDescriptor worker = worker(101, "current");
        client.registerWorker(worker);
        assertStatus(Status.Code.ALREADY_EXISTS, () -> client.registerWorker(worker(101, "other")));
        assertStatus(Status.Code.ALREADY_EXISTS, () -> client.registerWorker(worker.toBuilder()
                .setEndpoint(worker.getEndpoint().toBuilder().setPort(1201)).build()));
        assertEquals(worker, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void firstWorkerHasNoNeighboursAndPeersRemainTypedAndDistinct() {
        WorkerDescriptor requester = worker(101, "requester");
        String epoch = client.registerWorker(requester).getRegistryEpoch();
        assertTrue(client.getInitialNeighbours(requester.getIdentity(), epoch).isEmpty());
        WorkerDescriptor second = worker(102, "second"), third = worker(103, "third");
        client.registerWorker(second);
        client.registerWorker(third);
        List<WorkerDescriptor> neighbours = client.getInitialNeighbours(requester.getIdentity(), epoch);
        assertEquals(2, neighbours.size());
        assertEquals(new HashSet<>(List.of(second, third)), new HashSet<>(neighbours));
        assertThrows(UnsupportedOperationException.class, neighbours::clear);
        client.deregisterWorker(third.getIdentity(), epoch);
        assertEquals(List.of(second), client.getInitialNeighbours(requester.getIdentity(), epoch));
    }

    @Test
    public void deregistrationIsIdempotentAndStaleSessionCannotRemoveReplacement() {
        WorkerDescriptor old = worker(101, "old");
        String epoch = client.registerWorker(old).getRegistryEpoch();
        var removed = client.deregisterWorker(old.getIdentity(), epoch);
        assertTrue(removed.getRemoved());
        assertEquals(epoch, removed.getRegistryEpoch());
        assertEquals(2, removed.getMembershipRevision());
        assertFalse(client.deregisterWorker(old.getIdentity(), epoch).getRemoved());
        WorkerDescriptor replacement = worker(101, "new");
        client.registerWorker(replacement);
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> client.deregisterWorker(old.getIdentity(), epoch));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> client.getInitialNeighbours(old.getIdentity(), epoch));
        assertEquals(replacement, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void invalidIdentityUnknownWorkerAndWrongEpochPreserveGrpcStatuses() {
        StatusRuntimeException invalid = assertThrows(StatusRuntimeException.class,
                () -> client.registerWorker(worker(0, "invalid")));
        assertEquals(Status.Code.INVALID_ARGUMENT, invalid.getStatus().getCode());
        assertFalse(invalid.getStatus().getDescription().isBlank());
        WorkerDescriptor worker = worker(101, "session");
        String epoch = client.registerWorker(worker).getRegistryEpoch();
        assertStatus(Status.Code.NOT_FOUND, () -> client.getInitialNeighbours(worker(999, "unknown").getIdentity(), epoch));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> client.getInitialNeighbours(worker.getIdentity(), "old-epoch"));
        assertStatus(Status.Code.FAILED_PRECONDITION, () -> client.deregisterWorker(worker.getIdentity(), "old-epoch"));
        assertEquals(worker, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void closedClientRejectsCallsAndClosingDoesNotDeregisterWorkers() {
        WorkerDescriptor worker = worker(101, "session");
        client.registerWorker(worker);
        client.close();
        client.close();
        assertStatus(Status.Code.UNAVAILABLE, () -> client.registerWorker(worker(102, "next")));
        assertEquals(List.of(worker), registry.snapshot());
    }

    @Test
    public void serverShutdownPropagatesUnavailable() {
        client.registerWorker(worker(101, "session"));
        server.close();
        assertStatus(Status.Code.UNAVAILABLE, () -> client.registerWorker(worker(102, "next")));
    }

    @Test
    public void rpcDeadlineBoundsAnUnansweredRegistration() throws Exception {
        Server stalled = ServerBuilder.forPort(0).addService(new BootstrapServiceGrpc.BootstrapServiceImplBase() {
            @Override
            public void registerWorker(RegisterWorkerRequest request, StreamObserver<RegisterWorkerResponse> observer) {
                // Leave the call open to exercise the client's deadline.
            }
        }).build().start();
        try (var bounded = new BootstrapGrpcClient("127.0.0.1", stalled.getPort(), 500)) {
            assertStatus(Status.Code.DEADLINE_EXCEEDED, () -> bounded.registerWorker(worker(101, "session")));
        } finally {
            assertTrue(stalled.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    public void invalidConnectionSettingsFailBeforeOpeningAChannel() {
        assertThrows(IllegalArgumentException.class, () -> new BootstrapGrpcClient(null, server.getPort()));
        assertThrows(IllegalArgumentException.class, () -> new BootstrapGrpcClient(" ", server.getPort()));
        for (int port : new int[] { -1, 0, 65536 }) {
            assertThrows(IllegalArgumentException.class, () -> new BootstrapGrpcClient("127.0.0.1", port));
        }
        assertThrows(IllegalArgumentException.class, () -> new BootstrapGrpcClient("127.0.0.1", server.getPort(), 0));
    }

    private static void assertStatus(Status.Code expected, Runnable call) {
        assertEquals(expected, assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("worker-" + id).setPort(1101)).build();
    }
}
