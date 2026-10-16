package client.grpc;

import client.api.CoordinatorClient;
import client.api.JobProgressListener;
import client.model.JobSpec;
import common.grpc.BootstrapServiceGrpc;
import common.grpc.CoordinatorRecord;
import common.grpc.CoordinatorServiceGrpc;
import common.grpc.GetCoordinatorRequest;
import common.grpc.GetCoordinatorResponse;
import common.grpc.GetCoordinatorStatusRequest;
import common.grpc.GetCoordinatorStatusResponse;
import common.grpc.WatchJobRequest;
import common.grpc.WorkerEndpoint;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Talks to the cluster over gRPC, using the team's bootstrap.proto and
 * coordinator.proto.
 *
 * For every job:
 *  1. BootstrapService.GetCoordinator - ask the Bootstrap who the current
 *     coordinator is. Asked every time, because the coordinator changes
 *     after an election or a crash.
 *  2. CoordinatorService.SubmitJob    - hand the job to that coordinator.
 *     It answers straight away; the job runs in the background.
 *  3. CoordinatorService.WatchJob     - open the progress stream. The first
 *     message is a full snapshot, then one message per change, ending with
 *     COMPLETED or FAILED. JobProgressStreamHandler listens to it.
 *
 * submit() never blocks the caller: steps 1 and 2 are blocking unary calls
 * (with deadlines) run on a background thread, and step 3 uses gRPC's
 * asynchronous stub.
 */
public class GrpcCoordinatorClient implements CoordinatorClient {

    /** Deadline for the unary calls (GetCoordinator, SubmitJob, GetCoordinatorStatus). */
    private static final long CALL_TIMEOUT_SECONDS = 3;

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final ChannelFactory channelFactory;

    private final ManagedChannel bootstrapChannel;
    /** One reusable channel per coordinator address ("host:port"). */
    private final Map<String, ManagedChannel> coordinatorChannels = new ConcurrentHashMap<>();

    private final ExecutorService callPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "client-grpc-calls");
        t.setDaemon(true);
        return t;
    });

    public GrpcCoordinatorClient(String bootstrapHost, int bootstrapPort, ChannelFactory channelFactory) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.channelFactory = channelFactory;
        this.bootstrapChannel = channelFactory.open(bootstrapHost, bootstrapPort);
    }

    @Override
    public void submit(JobSpec job, JobProgressListener listener) {
        callPool.execute(() -> {
            // 1. Discovery
            CoordinatorRecord coordinator;
            try {
                coordinator = discoverCoordinator();
            } catch (StatusRuntimeException e) {
                listener.onFailed("Bootstrap unreachable at " + bootstrapHost + ":" + bootstrapPort
                        + " (" + JobProgressStreamHandler.describe(e.getStatus()) + ")",
                        JobProgressStreamHandler.isRetryable(e.getStatus().getCode()));
                return;
            }
            if (coordinator == null) {
                listener.onFailed("No coordinator has been elected yet", true);
                return;
            }

            String address = addressOf(coordinator);
            ManagedChannel channel = channelFor(coordinator);

            // 2. Submit (same job ID on every retry = "exact retry", no new work)
            try {
                CoordinatorServiceGrpc.newBlockingStub(channel)
                        .withDeadlineAfter(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .submitJob(ProtoMapper.toRequest(job));
            } catch (StatusRuntimeException e) {
                boolean retryable = JobProgressStreamHandler.isRetryable(e.getStatus().getCode());
                if (retryable) {
                    dropChannel(address);
                }
                listener.onFailed("Coordinator Worker " + idOf(coordinator) + " did not accept the job ("
                        + JobProgressStreamHandler.describe(e.getStatus()) + ")", retryable);
                return;
            }

            // 3. Watch the progress stream
            CoordinatorServiceGrpc.newStub(channel).watchJob(
                    WatchJobRequest.newBuilder().setJobId(job.jobId()).build(),
                    new JobProgressStreamHandler(listener, () -> dropChannel(address)));
        });
    }

    @Override
    public String checkConnection() throws Exception {
        CoordinatorRecord coordinator = discoverCoordinator();
        if (coordinator == null) {
            throw new IllegalStateException("Bootstrap is reachable, but no coordinator has been elected yet");
        }
        GetCoordinatorStatusResponse status = CoordinatorServiceGrpc.newBlockingStub(channelFor(coordinator))
                .withDeadlineAfter(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getCoordinatorStatus(GetCoordinatorStatusRequest.getDefaultInstance());
        return String.format("Coordinator Worker %d at %s (term %d, %d of %d jobs used this term)",
                status.getCoordinator().getWorkerId(), addressOf(coordinator), status.getTerm(),
                status.getJobsInTerm(), status.getMaxJobsPerTerm());
    }

    @Override
    public void close() {
        callPool.shutdownNow();
        bootstrapChannel.shutdownNow();
        coordinatorChannels.values().forEach(ManagedChannel::shutdownNow);
        coordinatorChannels.clear();
    }

    /**
     * Blocking unary call to the Bootstrap, with a deadline.
     *
     * @return the published coordinator, or null if none is published yet
     *         (bootstrap.proto: an absent record is a normal answer)
     */
    private CoordinatorRecord discoverCoordinator() {
        GetCoordinatorResponse response = BootstrapServiceGrpc.newBlockingStub(bootstrapChannel)
                .withDeadlineAfter(CALL_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                .getCoordinator(GetCoordinatorRequest.getDefaultInstance());
        return response.hasCoordinator() ? response.getCoordinator() : null;
    }

    private ManagedChannel channelFor(CoordinatorRecord coordinator) {
        WorkerEndpoint endpoint = coordinator.getCoordinator().getEndpoint();
        return coordinatorChannels.computeIfAbsent(addressOf(coordinator),
                key -> channelFactory.open(endpoint.getHost(), endpoint.getPort()));
    }

    /** Forget a channel whose coordinator stopped answering; the next job opens a fresh one. */
    private void dropChannel(String address) {
        ManagedChannel dead = coordinatorChannels.remove(address);
        if (dead != null) {
            dead.shutdownNow();
        }
    }

    private static String addressOf(CoordinatorRecord coordinator) {
        WorkerEndpoint endpoint = coordinator.getCoordinator().getEndpoint();
        return endpoint.getHost() + ":" + endpoint.getPort();
    }

    private static int idOf(CoordinatorRecord coordinator) {
        return coordinator.getCoordinator().getIdentity().getWorkerId();
    }
}
