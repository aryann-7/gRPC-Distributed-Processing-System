package client.grpc;

import client.api.CoordinatorClient;
import client.api.JobProgressListener;
import client.model.JobSpec;
import common.grpc.client.ClusterDiscoveryGrpc;
import common.grpc.client.CoordinatorEndpoint;
import common.grpc.client.CoordinatorJobServiceGrpc;
import common.grpc.client.CoordinatorQuery;
import common.grpc.client.CoordinatorStatus;
import common.grpc.client.StatusRequest;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Talks to the cluster over gRPC.
 *
 * For every job:
 *  1. Ask the Bootstrap (ClusterDiscovery.GetCoordinator) where the current
 *     coordinator is. Asked every time, because the coordinator changes
 *     after an election or a crash.
 *  2. Call CoordinatorJobService.SubmitJob on that coordinator and listen to
 *     the progress stream with a JobProgressStreamHandler.
 *
 * submit() never blocks the caller: the blocking discovery step runs on a
 * background thread, and the job itself uses gRPC's asynchronous stub.
 */
public class GrpcCoordinatorClient implements CoordinatorClient {

    private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

    private final String clientId;
    private final String bootstrapHost;
    private final int bootstrapPort;
    private final ChannelFactory channelFactory;

    private final ManagedChannel bootstrapChannel;
    /** One reusable channel per coordinator address ("host:port"). */
    private final Map<String, ManagedChannel> coordinatorChannels = new ConcurrentHashMap<>();

    private final ExecutorService discoveryPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "client-grpc-discovery");
        t.setDaemon(true);
        return t;
    });

    public GrpcCoordinatorClient(String clientId, String bootstrapHost, int bootstrapPort,
                                 ChannelFactory channelFactory) {
        this.clientId = clientId;
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.channelFactory = channelFactory;
        this.bootstrapChannel = channelFactory.open(bootstrapHost, bootstrapPort);
    }

    @Override
    public void submit(JobSpec job, JobProgressListener listener) {
        discoveryPool.execute(() -> {
            CoordinatorEndpoint endpoint;
            try {
                endpoint = discoverCoordinator();
            } catch (StatusRuntimeException e) {
                listener.onFailed("Bootstrap unreachable at " + bootstrapHost + ":" + bootstrapPort
                        + " (" + e.getStatus().getCode() + ")",
                        JobProgressStreamHandler.isRetryable(e.getStatus().getCode()));
                return;
            }
            if (!endpoint.getAvailable()) {
                listener.onFailed("No coordinator is elected yet", true);
                return;
            }

            String address = addressOf(endpoint);
            ManagedChannel channel = channelFor(endpoint);
            CoordinatorJobServiceGrpc.newStub(channel).submitJob(
                    ProtoMapper.toSubmission(job, clientId),
                    new JobProgressStreamHandler(listener, () -> dropChannel(address)));
        });
    }

    @Override
    public String checkConnection() throws Exception {
        CoordinatorEndpoint endpoint = discoverCoordinator();
        if (!endpoint.getAvailable()) {
            throw new IllegalStateException("Bootstrap is reachable, but no coordinator is elected yet");
        }
        CoordinatorStatus status = CoordinatorJobServiceGrpc.newBlockingStub(channelFor(endpoint))
                .withDeadlineAfter(DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getStatus(StatusRequest.newBuilder().setClientId(clientId).build());
        return String.format("Coordinator Worker %d at %s (term %d, %d active workers, %d jobs this term)",
                status.getCoordinatorId(), addressOf(endpoint), status.getTerm(),
                status.getActiveWorkers(), status.getJobsInTerm());
    }

    @Override
    public void close() {
        discoveryPool.shutdownNow();
        bootstrapChannel.shutdownNow();
        coordinatorChannels.values().forEach(ManagedChannel::shutdownNow);
        coordinatorChannels.clear();
    }

    /** Blocking unary call to the Bootstrap, with a timeout. */
    private CoordinatorEndpoint discoverCoordinator() {
        return ClusterDiscoveryGrpc.newBlockingStub(bootstrapChannel)
                .withDeadlineAfter(DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getCoordinator(CoordinatorQuery.newBuilder().setClientId(clientId).build());
    }

    private ManagedChannel channelFor(CoordinatorEndpoint endpoint) {
        return coordinatorChannels.computeIfAbsent(addressOf(endpoint),
                key -> channelFactory.open(endpoint.getHost(), endpoint.getPort()));
    }

    /** Forget a channel whose coordinator stopped answering; the next job opens a fresh one. */
    private void dropChannel(String address) {
        ManagedChannel dead = coordinatorChannels.remove(address);
        if (dead != null) {
            dead.shutdownNow();
        }
    }

    private static String addressOf(CoordinatorEndpoint endpoint) {
        return endpoint.getHost() + ":" + endpoint.getPort();
    }
}
