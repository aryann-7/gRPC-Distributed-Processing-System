package client.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import client.api.JobProgressListener;
import client.api.RetryingCoordinatorClient;
import client.model.JobKind;
import client.model.JobSpec;
import client.progress.WorkerProgress;
import common.grpc.client.ClusterDiscoveryGrpc;
import common.grpc.client.CoordinatorEndpoint;
import common.grpc.client.CoordinatorJobServiceGrpc;
import common.grpc.client.CoordinatorQuery;
import common.grpc.client.CoordinatorStatus;
import common.grpc.client.JobProgress;
import common.grpc.client.JobSubmission;
import common.grpc.client.StatusRequest;
import common.grpc.client.WorkerTaskProgress;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Runs the REAL GrpcCoordinatorClient against fake Bootstrap and Coordinator
 * servers over gRPC's in-process transport (real gRPC calls, no network
 * ports). The fakes play scripted messages so each client behaviour can be
 * checked: progress streaming, results, failures, and failover.
 */
public class GrpcCoordinatorClientTest {

    private static final JobSpec JOB = JobSpec.forRange("job-test", 1, 20);

    /** Channel factory: "host:port" becomes the in-process server name. */
    private static final ChannelFactory IN_PROCESS =
            (host, port) -> InProcessChannelBuilder.forName(host + ":" + port).directExecutor().build();

    private final String run = UUID.randomUUID().toString().substring(0, 8);
    private final List<Server> servers = new ArrayList<>();
    private FakeDiscovery discovery;

    // ---------------------------------------------------------------- fakes

    /** Fake Bootstrap: answers GetCoordinator with whatever endpoint is set. */
    private static class FakeDiscovery extends ClusterDiscoveryGrpc.ClusterDiscoveryImplBase {
        volatile CoordinatorEndpoint endpoint = CoordinatorEndpoint.newBuilder().setAvailable(false).build();

        @Override
        public void getCoordinator(CoordinatorQuery request, StreamObserver<CoordinatorEndpoint> out) {
            out.onNext(endpoint);
            out.onCompleted();
        }
    }

    /** What a fake coordinator does when it receives a job. */
    private interface Script {
        void run(JobSubmission request, StreamObserver<JobProgress> out);
    }

    /** Fake coordinator that runs a script and records the job IDs it saw. */
    private static class FakeCoordinator extends CoordinatorJobServiceGrpc.CoordinatorJobServiceImplBase {
        final Script script;
        final int id;
        final List<String> jobIdsSeen = new CopyOnWriteArrayList<>();

        FakeCoordinator(int id, Script script) {
            this.id = id;
            this.script = script;
        }

        @Override
        public void submitJob(JobSubmission request, StreamObserver<JobProgress> out) {
            jobIdsSeen.add(request.getJobId());
            script.run(request, out);
        }

        @Override
        public void getStatus(StatusRequest request, StreamObserver<CoordinatorStatus> out) {
            out.onNext(CoordinatorStatus.newBuilder().setCoordinatorId(id).setTerm(2)
                    .setActiveWorkers(3).setJobsInTerm(1).build());
            out.onCompleted();
        }
    }

    /** Records what the client reports. */
    private static class Recorder implements JobProgressListener {
        final CountDownLatch done = new CountDownLatch(1);
        final List<WorkerProgress> progress = new CopyOnWriteArrayList<>();
        final List<Integer> retries = new CopyOnWriteArrayList<>();
        volatile Long result;
        volatile String failure;
        volatile boolean retryable;

        @Override public void onProgress(WorkerProgress p) { progress.add(p); }
        @Override public void onCompleted(long r) { result = r; done.countDown(); }
        @Override public void onFailed(String m, boolean retry) { failure = m; retryable = retry; done.countDown(); }
        @Override public void onRetry(int attempt, int max, String reason) { retries.add(attempt); }

        void await() throws InterruptedException {
            assertTrue(done.await(10, TimeUnit.SECONDS), "job did not finish in time");
        }
    }

    // ---------------------------------------------------------------- setup

    @BeforeEach
    public void startBootstrap() throws Exception {
        discovery = new FakeDiscovery();
        start("boot-" + run + ":1", discovery);
    }

    @AfterEach
    public void stopServers() {
        servers.forEach(Server::shutdownNow);
    }

    private void start(String name, io.grpc.BindableService service) throws Exception {
        servers.add(InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start());
    }

    /** Starts a fake coordinator and returns the endpoint pointing at it. */
    private CoordinatorEndpoint startCoordinator(FakeCoordinator coordinator) throws Exception {
        String host = "coord" + coordinator.id + "-" + run;
        start(host + ":1", coordinator);
        return CoordinatorEndpoint.newBuilder().setAvailable(true).setHost(host).setPort(1)
                .setCoordinatorId(coordinator.id).setTerm(2).build();
    }

    private GrpcCoordinatorClient newClient() {
        return new GrpcCoordinatorClient("test-client", "boot-" + run, 1, IN_PROCESS);
    }

    private static WorkerTaskProgress task(int worker, long done, long total) {
        return WorkerTaskProgress.newBuilder().setWorkerId(worker).setTaskId("t" + worker)
                .setUnitsCompleted(done).setTotalUnits(total)
                .setStatus(done == total ? "COMPLETED" : "PROCESSING").build();
    }

    // ---------------------------------------------------------------- tests

    @Test
    public void streamsWorkerProgressThenResult() throws Exception {
        discovery.endpoint = startCoordinator(new FakeCoordinator(103, (req, out) -> {
            out.onNext(JobProgress.newBuilder().setJobId(req.getJobId())
                    .setState(JobProgress.JobState.RUNNING)
                    .addWorkers(task(101, 5, 10)).addWorkers(task(102, 0, 10)).build());
            out.onNext(JobProgress.newBuilder().setJobId(req.getJobId())
                    .setState(JobProgress.JobState.COMPLETED).setResultValue(77)
                    .addWorkers(task(101, 10, 10)).addWorkers(task(102, 10, 10)).build());
            out.onCompleted();
        }));

        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertEquals(77L, (long) rec.result);
        assertEquals(4, rec.progress.size());
        assertEquals(new WorkerProgress(101, "t101", 5, 10, "PROCESSING"), rec.progress.get(0));
    }

    @Test
    public void noCoordinatorElectedIsRetryable() throws Exception {
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
        assertTrue(rec.failure.contains("No coordinator"));
    }

    @Test
    public void unreachableCoordinatorIsRetryable() throws Exception {
        // Discovery points at a coordinator that is not running.
        discovery.endpoint = CoordinatorEndpoint.newBuilder().setAvailable(true)
                .setHost("dead-" + run).setPort(1).setCoordinatorId(9).build();
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
        assertTrue(rec.failure.contains("UNAVAILABLE"));
    }

    @Test
    public void unreachableBootstrapIsRetryable() throws Exception {
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client =
                     new GrpcCoordinatorClient("c", "no-bootstrap-" + run, 1, IN_PROCESS)) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
        assertTrue(rec.failure.contains("Bootstrap unreachable"));
    }

    @Test
    public void coordinatorRejectionIsNotRetried() throws Exception {
        discovery.endpoint = startCoordinator(new FakeCoordinator(103, (req, out) -> {
            out.onNext(JobProgress.newBuilder().setJobId(req.getJobId())
                    .setState(JobProgress.JobState.FAILED).setErrorMessage("Invalid range")
                    .setRetryable(false).build());
            out.onCompleted();
        }));
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertEquals("Invalid range", rec.failure);
        assertFalse(rec.retryable);
    }

    @Test
    public void streamClosedWithoutResultIsRetryable() throws Exception {
        discovery.endpoint = startCoordinator(new FakeCoordinator(103, (req, out) -> out.onCompleted()));
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
    }

    @Test
    public void failsOverToNewCoordinatorWithSameJobId() throws Exception {
        FakeCoordinator replacement = new FakeCoordinator(102, (req, out) -> {
            out.onNext(JobProgress.newBuilder().setJobId(req.getJobId())
                    .setState(JobProgress.JobState.COMPLETED).setResultValue(77)
                    .addWorkers(task(101, 10, 10)).build());
            out.onCompleted();
        });
        CoordinatorEndpoint replacementEndpoint = startCoordinator(replacement);

        // First coordinator sends some progress, then "crashes"; meanwhile the
        // cluster elects the replacement, which discovery now returns.
        FakeCoordinator crashing = new FakeCoordinator(103, (req, out) -> {
            out.onNext(JobProgress.newBuilder().setJobId(req.getJobId())
                    .setState(JobProgress.JobState.RUNNING).addWorkers(task(101, 3, 10)).build());
            discovery.endpoint = replacementEndpoint;
            out.onError(Status.UNAVAILABLE.withDescription("coordinator crashed").asRuntimeException());
        });
        discovery.endpoint = startCoordinator(crashing);

        Recorder rec = new Recorder();
        try (RetryingCoordinatorClient client = new RetryingCoordinatorClient(newClient(), 3, 50)) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertEquals(77L, (long) rec.result);
        assertEquals(List.of(2), rec.retries);
        assertEquals(List.of("job-test"), crashing.jobIdsSeen);
        assertEquals(List.of("job-test"), replacement.jobIdsSeen);
    }

    @Test
    public void checkConnectionDescribesCoordinator() throws Exception {
        discovery.endpoint = startCoordinator(new FakeCoordinator(103, (req, out) -> out.onCompleted()));
        try (GrpcCoordinatorClient client = newClient()) {
            String found = client.checkConnection();
            assertTrue(found.contains("Coordinator Worker 103"), found);
            assertTrue(found.contains("3 active workers"), found);
        }
    }

    @Test
    public void checkConnectionFailsWhenNoCoordinator() {
        try (GrpcCoordinatorClient client = newClient()) {
            assertThrows(IllegalStateException.class, client::checkConnection);
        }
    }

    @Test
    public void listJobsAreSentWithTheirNumbers() throws Exception {
        List<List<Long>> received = new CopyOnWriteArrayList<>();
        discovery.endpoint = startCoordinator(new FakeCoordinator(103, (req, out) -> {
            received.add(req.getNumbersList());
            out.onNext(JobProgress.newBuilder().setJobId(req.getJobId())
                    .setState(JobProgress.JobState.COMPLETED).setResultValue(99).build());
            out.onCompleted();
        }));
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JobSpec.forNumbers("job-max", JobKind.MAX, List.of(3L, 99L, 7L)), rec);
            rec.await();
        }
        assertEquals(List.of(List.of(3L, 99L, 7L)), received);
        assertEquals(99L, (long) rec.result);
    }
}
