package worker.grpc;

import common.grpc.AddNeighbourRequest;
import common.grpc.AddNeighbourResponse;
import common.grpc.PeerServiceGrpc;
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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class PeerGrpcClientTest {
    private final ArrayBlockingQueue<AddNeighbourRequest> requests = new ArrayBlockingQueue<>(4);
    private volatile WorkerDescriptor stored;
    private volatile Status rejection;
    private volatile boolean stalled;
    private Server server;
    private PeerGrpcClient client;

    @BeforeEach
    public void setUp() throws Exception {
        server = ServerBuilder.forPort(0).addService(new PeerServiceGrpc.PeerServiceImplBase() {
            @Override
            public void addNeighbour(AddNeighbourRequest request, StreamObserver<AddNeighbourResponse> observer) {
                requests.offer(request);
                if (rejection != null) {
                    observer.onError(rejection.asRuntimeException());
                } else if (!stalled) {
                    observer.onNext(AddNeighbourResponse.newBuilder()
                            .setNeighbour(stored == null ? request.getNeighbour() : stored).build());
                    observer.onCompleted();
                }
            }
        }).build().start();
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
    public void repeatedCallsTransmitDescriptorsAndReturnThePeersActualResponse() throws Exception {
        WorkerDescriptor neighbour = worker(101, "current-session");
        assertEquals(neighbour, client.addNeighbour(neighbour).getNeighbour());
        assertEquals(neighbour, requests.poll(5, TimeUnit.SECONDS).getNeighbour());
        stored = neighbour;
        WorkerDescriptor restarted = worker(101, "new-session");
        assertEquals(neighbour, client.addNeighbour(restarted).getNeighbour());
        assertEquals(restarted, requests.poll(5, TimeUnit.SECONDS).getNeighbour());
    }

    @Test
    public void serverErrorsPreserveStatusAndDescription() {
        rejection = Status.FAILED_PRECONDITION.withDescription("Peer rejected neighbour");
        StatusRuntimeException error = assertThrows(StatusRuntimeException.class,
                () -> client.addNeighbour(worker(101, "session")));
        assertEquals(rejection.getCode(), error.getStatus().getCode());
        assertEquals(rejection.getDescription(), error.getStatus().getDescription());
    }

    @Test
    public void unavailablePeerDoesNotReportSuccessfulRegistration() throws Exception {
        int port = server.getPort();
        assertTrue(server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS));
        try (var disconnected = new PeerGrpcClient("127.0.0.1", port)) {
            assertStatus(Status.Code.UNAVAILABLE, () -> disconnected.addNeighbour(worker(101, "session")));
        }
        assertTrue(requests.isEmpty());
    }

    @Test
    public void deadlineEndsAnUnansweredCall() {
        stalled = true;
        try (var bounded = new PeerGrpcClient("127.0.0.1", server.getPort(), 500)) {
            assertStatus(Status.Code.DEADLINE_EXCEEDED, () -> bounded.addNeighbour(worker(101, "session")));
        }
    }

    @Test
    public void closingIsIdempotentAndPreventsFurtherCalls() {
        client.addNeighbour(worker(101, "session"));
        client.close();
        client.close();
        assertStatus(Status.Code.UNAVAILABLE, () -> client.addNeighbour(worker(102, "other")));
        assertEquals(1, requests.size());
    }

    @Test
    public void invalidConnectionSettingsAreRejectedBeforeOpeningAChannel() {
        assertThrows(IllegalArgumentException.class, () -> new PeerGrpcClient(null, server.getPort()));
        assertThrows(IllegalArgumentException.class, () -> new PeerGrpcClient(" ", server.getPort()));
        for (int port : new int[] { -1, 0, 65536 }) {
            assertThrows(IllegalArgumentException.class, () -> new PeerGrpcClient("127.0.0.1", port));
        }
        assertThrows(IllegalArgumentException.class, () -> new PeerGrpcClient("127.0.0.1", server.getPort(), 0));
    }

    private static void assertStatus(Status.Code expected, Runnable call) {
        assertEquals(expected, assertThrows(StatusRuntimeException.class, call::run).getStatus().getCode());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("worker-" + id).setPort(1101)).build();
    }
}
