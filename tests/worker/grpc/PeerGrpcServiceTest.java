package worker.grpc;

import common.grpc.AddNeighbourRequest;
import common.grpc.PeerServiceGrpc;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class PeerGrpcServiceTest {
    private final ConcurrentHashMap<Integer, WorkerDescriptor> neighbours = new ConcurrentHashMap<>();
    private Server server;
    private PeerGrpcClient client;

    @BeforeEach
    public void setUp() throws Exception {
        server = ServerBuilder.forPort(0).addService(new PeerGrpcService(100, neighbours)).build().start();
        client = new PeerGrpcClient("127.0.0.1", server.getPort());
    }

    @AfterEach
    public void tearDown() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            if (server != null) {
                assertTrue(server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    public void registrationAndIdenticalRetriesPreserveTheFullDescriptor() {
        WorkerDescriptor neighbour = worker(101, "session");
        assertEquals(neighbour, client.addNeighbour(neighbour).getNeighbour());
        assertEquals(neighbour, client.addNeighbour(neighbour.toBuilder().build()).getNeighbour());
        assertEquals(1, neighbours.size());
        assertEquals(neighbour, neighbours.get(101));
    }

    @Test
    public void selfRegistrationIsRejectedRegardlessOfSession() {
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> client.addNeighbour(worker(100, "self")));
        assertStatus(Status.Code.INVALID_ARGUMENT, () -> client.addNeighbour(worker(100, "other-session")));
        assertTrue(neighbours.isEmpty());
    }

    @Test
    public void invalidOrMissingDescriptorsCannotChangeTopology() throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
        try {
            var stub = PeerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS);
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> stub.addNeighbour(AddNeighbourRequest.getDefaultInstance()));
        } finally {
            assertTrue(channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        }
        WorkerDescriptor valid = worker(101, "session");
        for (WorkerDescriptor invalid : List.of(valid.toBuilder().clearIdentity().build(),
                valid.toBuilder().clearEndpoint().build(), worker(0, "session"), worker(-1, "session"), worker(101, " "))) {
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> client.addNeighbour(invalid));
        }
        for (int port : new int[] { 0, -1, 65536 }) {
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> client.addNeighbour(valid.toBuilder()
                    .setEndpoint(valid.getEndpoint().toBuilder().setPort(port)).build()));
        }
        for (String host : new String[] { "", "bad host", "http://host", "host:1101", "host/path",
                "host?query", "user@host", "0.0.0.0", "::", "[::1]" }) {
            assertStatus(Status.Code.INVALID_ARGUMENT, () -> client.addNeighbour(valid.toBuilder()
                    .setEndpoint(valid.getEndpoint().toBuilder().setHost(host)).build()));
        }
        assertTrue(neighbours.isEmpty());
        WorkerDescriptor ipv6 = valid.toBuilder().setEndpoint(WorkerEndpoint.newBuilder()
                .setHost("2001:db8::1").setPort(65535)).build();
        assertEquals(ipv6, client.addNeighbour(ipv6).getNeighbour());
    }

    @Test
    public void conflictingSessionOrEndpointCannotOverwriteAnExistingNeighbour() {
        WorkerDescriptor original = worker(101, "original");
        client.addNeighbour(original);
        assertStatus(Status.Code.ALREADY_EXISTS, () -> client.addNeighbour(worker(101, "new-session")));
        assertStatus(Status.Code.ALREADY_EXISTS, () -> client.addNeighbour(original.toBuilder()
                .setEndpoint(original.getEndpoint().toBuilder().setPort(1201)).build()));
        assertEquals(original, neighbours.get(101));
        assertEquals(original, client.addNeighbour(original).getNeighbour());
    }

    @Test
    public void topologyOwnerAndServiceUseTheSameMap() {
        WorkerDescriptor original = worker(101, "original");
        neighbours.put(101, original);
        assertStatus(Status.Code.ALREADY_EXISTS, () -> client.addNeighbour(worker(101, "replacement")));
        assertTrue(neighbours.remove(101, original));
        WorkerDescriptor replacement = worker(101, "replacement");
        assertEquals(replacement, client.addNeighbour(replacement).getNeighbour());
        assertEquals(replacement, neighbours.get(101));
    }

    @Test
    public void concurrentSessionsHaveOneWinnerAndExactRetriesRemainSafe() throws Exception {
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        WorkerDescriptor first = worker(101, "first"), second = worker(101, "second");
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                WorkerDescriptor candidate = i % 2 == 0 ? first : second;
                results.add(executor.submit(() -> {
                    start.await();
                    try {
                        assertEquals(candidate, client.addNeighbour(candidate).getNeighbour());
                        return true;
                    } catch (StatusRuntimeException error) {
                        assertEquals(Status.Code.ALREADY_EXISTS, error.getStatus().getCode());
                        return false;
                    }
                }));
            }
            start.countDown();
            int accepted = 0;
            for (Future<Boolean> result : results) {
                if (result.get(5, TimeUnit.SECONDS)) {
                    accepted++;
                }
            }
            assertEquals(16, accepted);
            assertEquals(1, neighbours.size());
            assertTrue(List.of(first, second).contains(neighbours.get(101)));
        } finally {
            executor.shutdownNow();
        }
    }

    private static void assertStatus(Status.Code expected, Runnable call) {
        assertEquals(expected, assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("worker-" + id).setPort(1101)).build();
    }
}
