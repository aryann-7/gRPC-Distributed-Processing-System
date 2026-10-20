package worker.coordinator;

import common.grpc.*;
import common.models.JobResult;
import common.models.JobTask;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import worker.grpc.WorkerGrpcServer;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorServiceImplTest {

    private WorkerGrpcServer server;
    private ManagedChannel channel;
    private CoordinatorServiceGrpc.CoordinatorServiceBlockingStub blockingStub;
    private CoordinatorServiceGrpc.CoordinatorServiceStub asyncStub;

    private CoordinatorManager coordinatorManager;
    private final AtomicBoolean isCoordinator = new AtomicBoolean(false);
    private final int workerId = 42;
    private int port;

    @BeforeEach
    public void setUp() throws Exception {
        try (ServerSocket s = new ServerSocket(0)) {
            port = s.getLocalPort();
        }

        coordinatorManager = new CoordinatorManager(0);
        coordinatorManager.setCurrentTerm(3);

        server = new WorkerGrpcServer(
                workerId,
                port,
                4,
                () -> coordinatorManager,
                isCoordinator::get,
                () -> WorkerIdentity.newBuilder()
                        .setWorkerId(workerId)
                        .setSessionId("session-42")
                        .build()
        );
        server.start();

        channel = ManagedChannelBuilder.forAddress("localhost", port)
                .usePlaintext()
                .build();

        blockingStub = CoordinatorServiceGrpc.newBlockingStub(channel);
        asyncStub = CoordinatorServiceGrpc.newStub(channel);
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (channel != null) {
            channel.shutdownNow().awaitTermination(2, TimeUnit.SECONDS);
        }
        if (server != null) {
            server.stop();
        }
    }

    @Test
    public void testSubmitJobFailsIfNotCoordinator() {
        isCoordinator.set(false);

        SubmitJobRequest submission = SubmitJobRequest.newBuilder()
                .setJobId("job-fail-1")
                .setJobType(JobTypeProto.MAX)
                .setNumbers(JobNumbers.newBuilder().addAllNumbers(Arrays.asList(10L, 20L, 30L)).build())
                .build();

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> blockingStub.submitJob(submission));
        assertEquals(Status.Code.FAILED_PRECONDITION, ex.getStatus().getCode());
        assertTrue(ex.getMessage().contains("not the active coordinator"));
    }

    @Test
    public void testGetCoordinatorStatus() {
        isCoordinator.set(true);

        GetCoordinatorStatusRequest request = GetCoordinatorStatusRequest.newBuilder().build();
        GetCoordinatorStatusResponse response = blockingStub.getCoordinatorStatus(request);

        assertNotNull(response);
        assertEquals(workerId, response.getCoordinator().getWorkerId());
        assertEquals("session-42", response.getCoordinator().getSessionId());
        assertEquals(3, response.getTerm());
        assertEquals(5, response.getMaxJobsPerTerm());
        assertEquals(0, response.getJobsInTerm());
    }

    @Test
    public void testSubmitAndWatchJobProgress() throws Exception {
        isCoordinator.set(true);

        // Add a mock or local worker directly into coordinatorManager for computation
        coordinatorManager.addWorker(new common.interfaces.WorkerService() {
            @Override
            public void addNeighbour(int neighbourId, common.interfaces.WorkerService neighbour) {}

            @Override
            public JobResult executeJob(JobTask task) {
                long max = Long.MIN_VALUE;
                for (Long num : task.getDataChunk()) {
                    if (num > max) max = num;
                }
                return new JobResult(task.getJobId(), task.getTaskId(), true, max, null, 10L);
            }

            @Override
            public void receiveElectionMessage(common.models.ElectionMessage message) {}

            @Override
            public void receiveElectionReply(common.models.ElectionReply reply) {}

            @Override
            public boolean ping() {
                return true;
            }
        });

        SubmitJobRequest submission = SubmitJobRequest.newBuilder()
                .setJobId("job-success-1")
                .setJobType(JobTypeProto.MAX)
                .setNumbers(JobNumbers.newBuilder().addAllNumbers(Arrays.asList(15L, 88L, 42L, 99L, 12L)).build())
                .build();

        SubmitJobResponse ack = blockingStub.submitJob(submission);
        assertEquals("job-success-1", ack.getJobId());

        // Watch Job
        CountDownLatch latch = new CountDownLatch(1);
        List<JobProgress> progressEvents = new ArrayList<>();

        WatchJobRequest watchReq = WatchJobRequest.newBuilder()
                .setJobId("job-success-1")
                .build();

        asyncStub.watchJob(watchReq, new StreamObserver<JobProgress>() {
            @Override
            public void onNext(JobProgress value) {
                synchronized (progressEvents) {
                    progressEvents.add(value);
                }
            }

            @Override
            public void onError(Throwable t) {
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS), "WatchJob did not complete in time");
        assertFalse(progressEvents.isEmpty(), "Should have received progress events");

        // Last progress event should be COMPLETED
        JobProgress last = progressEvents.get(progressEvents.size() - 1);
        assertEquals(JobStateProto.COMPLETED, last.getState());
        assertEquals(99L, last.getResultValue());
        assertEquals(5, last.getUnitsCompleted());
        assertEquals(5, last.getTotalUnits());
        assertEquals(1, last.getWorkersCount(), "Should contain at least 1 worker in breakdown");
        assertEquals(5, last.getWorkers(0).getUnitsCompleted());
        assertEquals(5, last.getWorkers(0).getTotalUnits());

        // Verify getJobStatus unary
        GetJobStatusRequest statusReq = GetJobStatusRequest.newBuilder().setJobId("job-success-1").build();
        JobProgress statusResp = blockingStub.getJobStatus(statusReq);
        assertEquals(JobStateProto.COMPLETED, statusResp.getState());
        assertEquals(99L, statusResp.getResultValue());
        assertEquals(1, statusResp.getWorkersCount(), "Status response should also have worker breakdown");
    }

    @Test
    public void testSubmitJobInvalidDataRejected() {
        isCoordinator.set(true);

        SubmitJobRequest submission = SubmitJobRequest.newBuilder()
                .setJobId("job-empty-data")
                .setJobType(JobTypeProto.MAX)
                .build();

        StatusRuntimeException ex = assertThrows(StatusRuntimeException.class, () -> blockingStub.submitJob(submission));
        assertEquals(Status.Code.INVALID_ARGUMENT, ex.getStatus().getCode());
    }
}
