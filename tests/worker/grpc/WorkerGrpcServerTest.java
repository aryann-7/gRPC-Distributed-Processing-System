package worker.grpc;

import common.grpc.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerGrpcServerTest {

    private WorkerGrpcServer server;
    private ManagedChannel channel;
    private WorkerServiceGrpc.WorkerServiceStub asyncStub;
    private WorkerServiceGrpc.WorkerServiceBlockingStub blockingStub;
    private int testPort;
    private final int workerId = 101;

    @BeforeEach
    public void setUp() throws Exception {
        try (java.net.ServerSocket s = new java.net.ServerSocket(0)) {
            testPort = s.getLocalPort();
        }
        server = new WorkerGrpcServer(workerId, testPort, 4);
        server.start();

        channel = ManagedChannelBuilder.forAddress("localhost", testPort)
                .usePlaintext()
                .build();
        asyncStub = WorkerServiceGrpc.newStub(channel);
        blockingStub = WorkerServiceGrpc.newBlockingStub(channel);
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
    public void testHeartbeatPing() {
        HeartbeatRequest request = HeartbeatRequest.newBuilder()
                .setCoordinatorId(1)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        HeartbeatResponse response = blockingStub.ping(request);
        assertNotNull(response);
        assertEquals(workerId, response.getWorkerId());
        assertEquals(HeartbeatResponse.WorkerStatus.IDLE, response.getStatus());
        assertEquals(0, response.getActiveTasksCount());
    }

    @Test
    public void testExecuteTaskStreamProgress() throws Exception {
        List<Long> data = new ArrayList<>();
        for (long i = 1; i <= 1500; i++) {
            data.add(i);
        }

        TaskDispatchRequest request = TaskDispatchRequest.newBuilder()
                .setJobId("job-stream-test")
                .setTaskId("task-max-stream")
                .setJobType(JobTypeProto.MAX)
                .addAllDataChunk(data)
                .build();

        CountDownLatch latch = new CountDownLatch(1);
        List<TaskProgressUpdate> updates = new ArrayList<>();

        asyncStub.executeTaskStream(request, new StreamObserver<TaskProgressUpdate>() {
            @Override
            public void onNext(TaskProgressUpdate update) {
                synchronized (updates) {
                    updates.add(update);
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

        assertTrue(latch.await(5, TimeUnit.SECONDS), "Streaming did not complete in time");
        assertFalse(updates.isEmpty(), "Should have received streamed progress updates");

        TaskProgressUpdate last = updates.get(updates.size() - 1);
        assertEquals(100.0, last.getProgressPercentage(), 0.01);
        assertEquals("COMPLETED", last.getStatusMessage());
    }

    @Test
    public void testCancelTask() {
        CancelTaskRequest request = CancelTaskRequest.newBuilder()
                .setJobId("job-cancel")
                .setTaskId("task-non-existent")
                .setReason("Preemption")
                .build();

        CancelTaskResponse response = blockingStub.cancelTask(request);
        assertNotNull(response);
        assertFalse(response.getCancelled());
    }
}
