package worker.grpc;

import common.grpc.AddNeighbourRequest;
import common.grpc.AddNeighbourResponse;
import common.grpc.PeerServiceGrpc;
import common.grpc.WorkerDescriptor;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

public final class PeerGrpcService extends PeerServiceGrpc.PeerServiceImplBase {
    private final int workerId;
    private final ConcurrentMap<Integer, WorkerDescriptor> neighbours;

    // The topology owner supplies its shared map; this service does not keep a copy.
    public PeerGrpcService(int workerId, ConcurrentMap<Integer, WorkerDescriptor> neighbours) {
        if (workerId <= 0) {
            throw new IllegalArgumentException("Worker ID must be positive");
        }
        this.workerId = workerId;
        this.neighbours = Objects.requireNonNull(neighbours);
    }

    @Override
    public void addNeighbour(AddNeighbourRequest request, StreamObserver<AddNeighbourResponse> observer) {
        WorkerDescriptor neighbour = request.getNeighbour();
        WorkerDescriptor stored;
        try {
            validate(neighbour);
            stored = neighbours.putIfAbsent(neighbour.getIdentity().getWorkerId(), neighbour);
        } catch (IllegalArgumentException | URISyntaxException e) {
            observer.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
            return;
        }
        if (stored == null) {
            stored = neighbour;
        } else if (!stored.equals(neighbour)) {
            observer.onError(Status.ALREADY_EXISTS.withDescription("Neighbour ID already has a different registration")
                    .asRuntimeException());
            return;
        }
        observer.onNext(AddNeighbourResponse.newBuilder().setNeighbour(stored).build());
        observer.onCompleted();
    }

    private void validate(WorkerDescriptor neighbour) throws URISyntaxException {
        if (!neighbour.hasIdentity() || !neighbour.hasEndpoint()) {
            throw new IllegalArgumentException("Neighbour identity and endpoint are required");
        }
        int id = neighbour.getIdentity().getWorkerId();
        if (id <= 0 || id == workerId || neighbour.getIdentity().getSessionId().isBlank()) {
            throw new IllegalArgumentException("Neighbour must have a positive non-self ID and a session");
        }
        String host = neighbour.getEndpoint().getHost();
        int port = neighbour.getEndpoint().getPort();
        if (port < 1 || port > 65535 || host.isBlank() || host.contains("[") || host.contains("]")
                || host.equals("0.0.0.0") || host.matches("[0:]+")) {
            throw new IllegalArgumentException("Neighbour must advertise a valid host and port");
        }
        URI address = new URI("grpc", null, host, port, null, null, null).parseServerAuthority();
        if (!host.equals(address.getHost()) && !("[" + host + "]").equals(address.getHost())) {
            throw new IllegalArgumentException("Neighbour host must not contain URI components");
        }
    }
}
