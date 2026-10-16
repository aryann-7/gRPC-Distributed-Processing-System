package worker.grpc;

import common.grpc.AddNeighbourRequest;
import common.grpc.AddNeighbourResponse;
import common.grpc.PeerServiceGrpc;
import common.grpc.WorkerDescriptor;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.concurrent.TimeUnit;

public final class PeerGrpcClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final PeerServiceGrpc.PeerServiceBlockingStub stub;
    private final long timeoutMillis;

    public PeerGrpcClient(String host, int port) {
        this(host, port, 5_000);
    }

    public PeerGrpcClient(String host, int port, long timeoutMillis) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Peer host is required");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Peer port must be between 1 and 65535");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("RPC timeout must be positive");
        }
        this.timeoutMillis = timeoutMillis;
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().disableRetry().build();
        stub = PeerServiceGrpc.newBlockingStub(channel);
    }

    public AddNeighbourResponse addNeighbour(WorkerDescriptor neighbour) {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .addNeighbour(AddNeighbourRequest.newBuilder().setNeighbour(neighbour).build());
    }

    ManagedChannel channel() {
        return channel;
    }

    @Override
    public void close() {
        channel.shutdown();
        try {
            if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
                channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            channel.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
