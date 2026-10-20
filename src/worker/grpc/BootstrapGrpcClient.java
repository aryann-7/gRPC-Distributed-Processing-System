package worker.grpc;

import common.grpc.BootstrapServiceGrpc;
import common.grpc.DeregisterWorkerRequest;
import common.grpc.DeregisterWorkerResponse;
import common.grpc.GetInitialNeighboursRequest;
import common.grpc.RegisterWorkerRequest;
import common.grpc.RegisterWorkerResponse;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerIdentity;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.concurrent.TimeUnit;

public final class BootstrapGrpcClient implements AutoCloseable {
    private final ManagedChannel channel;
    private final BootstrapServiceGrpc.BootstrapServiceBlockingStub stub;
    private final long timeoutMillis;

    public BootstrapGrpcClient(String host, int port) {
        this(host, port, 5_000);
    }

    public BootstrapGrpcClient(String host, int port, long timeoutMillis) {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("Bootstrap host is required");
        }
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Bootstrap port must be between 1 and 65535");
        }
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("RPC timeout must be positive");
        }
        this.timeoutMillis = timeoutMillis;
        channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext().disableRetry().build();
        stub = BootstrapServiceGrpc.newBlockingStub(channel);
    }

    public RegisterWorkerResponse registerWorker(WorkerDescriptor worker) {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .registerWorker(RegisterWorkerRequest.newBuilder().setWorker(worker).build());
    }

    public List<WorkerDescriptor> getInitialNeighbours(WorkerIdentity requester, String registryEpoch) {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .getInitialNeighbours(GetInitialNeighboursRequest.newBuilder().setRequester(requester)
                        .setRegistryEpoch(registryEpoch).build()).getNeighboursList();
    }

    public DeregisterWorkerResponse deregisterWorker(WorkerIdentity worker, String registryEpoch) {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .deregisterWorker(DeregisterWorkerRequest.newBuilder().setWorker(worker)
                        .setRegistryEpoch(registryEpoch).build());
    }

    public common.grpc.ListWorkersResponse listWorkers() {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .listWorkers(common.grpc.ListWorkersRequest.getDefaultInstance());
    }

    public common.grpc.GetCoordinatorResponse getCoordinator() {
        return stub.withDeadlineAfter(timeoutMillis, TimeUnit.MILLISECONDS)
                .getCoordinator(common.grpc.GetCoordinatorRequest.getDefaultInstance());
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
