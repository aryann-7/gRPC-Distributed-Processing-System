package client.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import client.api.JobProgressListener;
import client.api.RetryingCoordinatorClient;
import client.model.JobKind;
import client.model.JobSpec;
import client.progress.ProgressSnapshot;
import client.progress.WorkerProgress;
import common.grpc.BootstrapServiceGrpc;
import common.grpc.CoordinatorRecord;
import common.grpc.CoordinatorServiceGrpc;
import common.grpc.GetCoordinatorRequest;
import common.grpc.GetCoordinatorResponse;
import common.grpc.GetCoordinatorStatusRequest;
import common.grpc.GetCoordinatorStatusResponse;
import common.grpc.JobProgress;
import common.grpc.JobStateProto;
import common.grpc.SubmitJobRequest;
import common.grpc.SubmitJobResponse;
import common.grpc.WatchJobRequest;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import common.grpc.WorkerJobProgress;
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
 * Runs the REAL GrpcCoordinatorClient against fake BootstrapService and
 * CoordinatorService servers (generated from the team's bootstrap.proto and
 * coordinator.proto) over gRPC's in-process transport: real gRPC calls, no
 * network ports. The fakes are test doubles only; the real services are
 * written by the Bootstrap and Coordinator owners.
 */
public class GrpcCoordinatorClientTest {

    private static final JobSpec JOB = JobSpec.forRange("job-test", 1, 20);

    /** Channel factory: "host:port" becomes the in-process server name. */
    private static final ChannelFactory IN_PROCESS =
            (host, port) -> InProcessChannelBuilder.forName(host + ":" + port).directExecutor().build();

    private final String run = UUID.randomUUID().toString().substring(0, 8);
    private final List<Server> servers = new ArrayList<>();
    private FakeBootstrap bootstrap;

    // ---------------------------------------------------------------- fakes

    /** Fake Bootstrap: answers GetCoordinator with whatever record is set (null = none). */
    private static class FakeBootstrap extends BootstrapServiceGrpc.BootstrapServiceImplBase {
        volatile CoordinatorRecord record;

        @Override
        public void getCoordinator(GetCoordinatorRequest request, StreamObserver<GetCoordinatorResponse> out) {
            GetCoordinatorResponse.Builder response = GetCoordinatorResponse.newBuilder().setRegistryEpoch("e1");
            if (record != null) {
                response.setCoordinator(record);
            }
            out.onNext(response.build());
            out.onCompleted();
        }
    }

    /** What a fake coordinator streams back from WatchJob. */
    private interface Script {
        void run(String jobId, StreamObserver<JobProgress> out);
    }

    /** Fake coordinator: records submitted jobs and plays a script on WatchJob. */
    private static class FakeCoordinator extends CoordinatorServiceGrpc.CoordinatorServiceImplBase {
        final int id;
        final Script script;
        final List<SubmitJobRequest> submitted = new CopyOnWriteArrayList<>();
        volatile Status rejectSubmitWith;

        FakeCoordinator(int id, Script script) {
            this.id = id;
            this.script = script;
        }

        @Override
        public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> out) {
            if (rejectSubmitWith != null) {
                out.onError(rejectSubmitWith.asRuntimeException());
                return;
            }
            submitted.add(request);
            out.onNext(SubmitJobResponse.newBuilder().setJobId(request.getJobId()).build());
            out.onCompleted();
        }

        @Override
        public void watchJob(WatchJobRequest request, StreamObserver<JobProgress> out) {
            script.run(request.getJobId(), out);
        }

        @Override
        public void getCoordinatorStatus(GetCoordinatorStatusRequest request,
                                         StreamObserver<GetCoordinatorStatusResponse> out) {
            out.onNext(GetCoordinatorStatusResponse.newBuilder()
                    .setCoordinator(identity(id)).setTerm(2)
                    .setJobsInTerm(1).setMaxJobsPerTerm(5).build());
            out.onCompleted();
        }
    }

    /** Records what the client reports. */
    private static class Recorder implements JobProgressListener {
        final CountDownLatch done = new CountDownLatch(1);
        final List<ProgressSnapshot> snapshots = new CopyOnWriteArrayList<>();
        final List<Integer> retries = new CopyOnWriteArrayList<>();
        volatile Long result;
        volatile String failure;
        volatile boolean retryable;

        @Override public void onProgress(ProgressSnapshot s) { snapshots.add(s); }
        @Override public void onCompleted(long r) { result = r; done.countDown(); }
        @Override public void onFailed(String m, boolean retry) { failure = m; retryable = retry; done.countDown(); }
        @Override public void onRetry(int attempt, int max, String reason) { retries.add(attempt); }

        void await() throws InterruptedException {
            assertTrue(done.await(10, TimeUnit.SECONDS), "job did not finish in time");
        }
    }

    // ---------------------------------------------------------------- helpers

    @BeforeEach
    public void startBootstrap() throws Exception {
        bootstrap = new FakeBootstrap();
        start("boot-" + run + ":1", bootstrap);
    }

    @AfterEach
    public void stopServers() {
        servers.forEach(Server::shutdownNow);
    }

    private void start(String name, io.grpc.BindableService service) throws Exception {
        servers.add(InProcessServerBuilder.forName(name).directExecutor().addService(service).build().start());
    }

    private static WorkerIdentity identity(int workerId) {
        return WorkerIdentity.newBuilder().setWorkerId(workerId).setSessionId("session-" + workerId).build();
    }

    private static CoordinatorRecord record(int workerId, String host) {
        return CoordinatorRecord.newBuilder().setTerm(2)
                .setCoordinator(WorkerDescriptor.newBuilder()
                        .setIdentity(identity(workerId))
                        .setEndpoint(WorkerEndpoint.newBuilder().setHost(host).setPort(1)))
                .build();
    }

    /** Starts a fake coordinator and returns the Bootstrap record pointing at it. */
    private CoordinatorRecord startCoordinator(FakeCoordinator coordinator) throws Exception {
        String host = "coord" + coordinator.id + "-" + run;
        start(host + ":1", coordinator);
        return record(coordinator.id, host);
    }

    private GrpcCoordinatorClient newClient() {
        return new GrpcCoordinatorClient("boot-" + run, 1, IN_PROCESS);
    }

    private static WorkerJobProgress worker(int id, long done, long total) {
        return WorkerJobProgress.newBuilder().setWorker(identity(id))
                .setUnitsCompleted(done).setTotalUnits(total).build();
    }

    private static JobProgress.Builder progress(String jobId, JobStateProto state, long done, long total) {
        return JobProgress.newBuilder().setJobId(jobId).setState(state)
                .setUnitsCompleted(done).setTotalUnits(total);
    }

    // ---------------------------------------------------------------- tests

    @Test
    public void submitsThenStreamsSnapshotsThenResult() throws Exception {
        FakeCoordinator coordinator = new FakeCoordinator(103, (jobId, out) -> {
            out.onNext(progress(jobId, JobStateProto.QUEUED, 0, 20).build());
            out.onNext(progress(jobId, JobStateProto.RUNNING, 5, 20)
                    .addWorkers(worker(101, 5, 10)).addWorkers(worker(102, 0, 10)).build());
            out.onNext(progress(jobId, JobStateProto.COMPLETED, 20, 20).setResultValue(77)
                    .addWorkers(worker(101, 10, 10)).addWorkers(worker(102, 10, 10)).build());
            out.onCompleted();
        });
        bootstrap.record = startCoordinator(coordinator);

        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertEquals(77L, (long) rec.result);
        assertEquals(1, coordinator.submitted.size());
        assertEquals(3, rec.snapshots.size());
        assertTrue(rec.snapshots.get(0).queued());
        assertEquals(5, rec.snapshots.get(1).unitsCompleted());
        assertEquals(new WorkerProgress(101, 5, 10), rec.snapshots.get(1).workers().get(0));
    }

    @Test
    public void noCoordinatorPublishedIsRetryable() throws Exception {
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
        bootstrap.record = record(9, "dead-" + run); // published, but not running
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
        assertTrue(rec.failure.contains("UNAVAILABLE"), rec.failure);
    }

    @Test
    public void unreachableBootstrapIsRetryable() throws Exception {
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = new GrpcCoordinatorClient("no-bootstrap-" + run, 1, IN_PROCESS)) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
        assertTrue(rec.failure.contains("Bootstrap unreachable"));
    }

    @Test
    public void inactiveCoordinatorIsRetryable() throws Exception {
        // coordinator.proto: "An inactive coordinator returns FAILED_PRECONDITION".
        FakeCoordinator coordinator = new FakeCoordinator(103, (jobId, out) -> out.onCompleted());
        coordinator.rejectSubmitWith = Status.FAILED_PRECONDITION.withDescription("not the coordinator");
        bootstrap.record = startCoordinator(coordinator);
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
        assertTrue(rec.failure.contains("FAILED_PRECONDITION"), rec.failure);
    }

    @Test
    public void invalidInputIsNotRetried() throws Exception {
        FakeCoordinator coordinator = new FakeCoordinator(103, (jobId, out) -> out.onCompleted());
        coordinator.rejectSubmitWith = Status.INVALID_ARGUMENT.withDescription("range too large");
        bootstrap.record = startCoordinator(coordinator);
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertFalse(rec.retryable);
        assertTrue(rec.failure.contains("range too large"), rec.failure);
    }

    @Test
    public void failedJobIsFinal() throws Exception {
        bootstrap.record = startCoordinator(new FakeCoordinator(103, (jobId, out) -> {
            out.onNext(progress(jobId, JobStateProto.FAILED, 3, 20).setErrorMessage("All workers lost").build());
            out.onCompleted();
        }));
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertEquals("All workers lost", rec.failure);
        assertFalse(rec.retryable);
    }

    @Test
    public void streamClosedWithoutResultIsRetryable() throws Exception {
        bootstrap.record = startCoordinator(new FakeCoordinator(103, (jobId, out) -> out.onCompleted()));
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertTrue(rec.retryable);
    }

    @Test
    public void failsOverToNewCoordinatorWithSameJobId() throws Exception {
        FakeCoordinator replacement = new FakeCoordinator(102, (jobId, out) -> {
            out.onNext(progress(jobId, JobStateProto.COMPLETED, 20, 20).setResultValue(77)
                    .addWorkers(worker(101, 20, 20)).build());
            out.onCompleted();
        });
        CoordinatorRecord replacementRecord = startCoordinator(replacement);

        // First coordinator streams some progress, then "crashes"; meanwhile the
        // cluster elects the replacement, which the Bootstrap now publishes.
        FakeCoordinator crashing = new FakeCoordinator(103, (jobId, out) -> {
            out.onNext(progress(jobId, JobStateProto.RUNNING, 3, 20).addWorkers(worker(101, 3, 10)).build());
            bootstrap.record = replacementRecord;
            out.onError(Status.UNAVAILABLE.withDescription("coordinator crashed").asRuntimeException());
        });
        bootstrap.record = startCoordinator(crashing);

        Recorder rec = new Recorder();
        try (RetryingCoordinatorClient client = new RetryingCoordinatorClient(newClient(), 3, 50)) {
            client.submit(JOB, rec);
            rec.await();
        }
        assertEquals(77L, (long) rec.result);
        assertEquals(List.of(2), rec.retries);
        assertEquals("job-test", crashing.submitted.get(0).getJobId());
        assertEquals("job-test", replacement.submitted.get(0).getJobId());
        assertEquals(crashing.submitted.get(0), replacement.submitted.get(0)); // exact retry
    }

    @Test
    public void checkConnectionDescribesCoordinator() throws Exception {
        bootstrap.record = startCoordinator(new FakeCoordinator(103, (jobId, out) -> out.onCompleted()));
        try (GrpcCoordinatorClient client = newClient()) {
            String found = client.checkConnection();
            assertTrue(found.contains("Coordinator Worker 103"), found);
            assertTrue(found.contains("term 2"), found);
            assertTrue(found.contains("1 of 5 jobs"), found);
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
        FakeCoordinator coordinator = new FakeCoordinator(103, (jobId, out) -> {
            out.onNext(progress(jobId, JobStateProto.COMPLETED, 3, 3).setResultValue(99).build());
            out.onCompleted();
        });
        bootstrap.record = startCoordinator(coordinator);
        Recorder rec = new Recorder();
        try (GrpcCoordinatorClient client = newClient()) {
            client.submit(JobSpec.forNumbers("job-max", JobKind.MAX, List.of(3L, 99L, 7L)), rec);
            rec.await();
        }
        assertEquals(List.of(3L, 99L, 7L), coordinator.submitted.get(0).getNumbers().getNumbersList());
        assertEquals(99L, (long) rec.result);
    }
}
