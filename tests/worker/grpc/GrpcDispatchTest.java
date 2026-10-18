package worker.grpc;

import common.grpc.*;
import common.models.JobRequest;
import common.models.JobType;
import org.junit.jupiter.api.Test;
import worker.coordinator.CoordinatorManager;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import io.grpc.ManagedChannelBuilder;
import static org.junit.jupiter.api.Assertions.*;

class GrpcDispatchTest {
    @Test
    void finalStreamResultPreservesAssignmentIdentity() throws Exception {
        WorkerGrpcServer server = new WorkerGrpcServer(201, 0, 1);
        server.start();
        var channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        try {
            var updates = WorkerServiceGrpc.newBlockingStub(channel).withDeadlineAfter(5, TimeUnit.SECONDS)
                    .executeTaskStream(TaskDispatchRequest.newBuilder().setJobId("retry").setTaskId("part-1")
                            .setJobType(JobTypeProto.MAX).addAllDataChunk(List.of(3L, 9L))
                            .setAttempt(2).setCoordinatorTerm(7).build());
            TaskProgressUpdate last = null;
            while (updates.hasNext()) last = updates.next();
            assertNotNull(last);
            assertEquals(2, last.getUnitsCompleted());
            assertEquals(2, last.getTotalUnits());
            assertEquals(2, last.getAttempt());
            assertEquals(7, last.getCoordinatorTerm());
            assertTrue(last.hasFinalResult());
            assertEquals("retry", last.getFinalResult().getJobId());
            assertEquals("part-1", last.getFinalResult().getTaskId());
            assertEquals(2, last.getFinalResult().getAttempt());
            assertEquals(7, last.getFinalResult().getCoordinatorTerm());
            assertEquals(9, last.getFinalResult().getResultValue());
        } finally {
            channel.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            server.stop();
        }
    }

    @Test
    void failedDispatchIsReportedWithoutIncreasingJac() throws Exception {
        WorkerGrpcServer server = new WorkerGrpcServer(201, 0, 1);
        server.start();
        int port = server.getPort();
        server.stop();
        try (WorkerGrpcClient worker = client(201, port);
             CoordinatorManager coordinator = new CoordinatorManager(0)) {
            coordinator.addWorker(worker);
            var result = coordinator.submitJob(new JobRequest("failed", JobType.MAX, List.of(2L)));
            assertFalse(result.isSuccess());
            assertTrue(result.getErrorMessage().contains("UNAVAILABLE"), result.getErrorMessage());
            assertEquals(0, coordinator.getPersistentJac());
        }
    }

    @Test
    void existingSchedulerDispatchesAllThreeJobsOverGrpc() throws Exception {
        WorkerGrpcServer first = new WorkerGrpcServer(201, 0, 2);
        WorkerGrpcServer second = new WorkerGrpcServer(202, 0, 2);
        try {
            first.start(); second.start();
            verifyJobs(first.getPort(), second.getPort());
        } finally { first.stop(); second.stop(); }
    }

    private static void verifyJobs(int firstPort, int secondPort) throws Exception {
            try (WorkerGrpcClient a = client(201, firstPort);
                 WorkerGrpcClient b = client(202, secondPort);
                 CoordinatorManager coordinator = new CoordinatorManager(0)) {
                coordinator.addWorker(a); coordinator.addWorker(b);
                List<TaskProgressUpdate> progress = new ArrayList<>();
                var max = coordinator.submitJob(new JobRequest("max", JobType.MAX, List.of(2L, 11L, -3L, 7L)),
                        (worker, update) -> { synchronized (progress) { progress.add(update); } });
                assertTrue(max.isSuccess(), max.getErrorMessage());
                assertEquals(11, max.getResultValue());
                assertTrue(progress.stream().anyMatch(TaskProgressUpdate::hasFinalResult));
                assertTrue(progress.stream().anyMatch(update -> update.getUnitsCompleted() > 0));
                var sum = coordinator.submitJob(new JobRequest("sum", JobType.PRIMESUM, 1, 10));
                assertTrue(sum.isSuccess(), sum.getErrorMessage());
                assertEquals(17, sum.getResultValue());
                var count = coordinator.submitJob(new JobRequest("count", JobType.PRIMECOUNT, List.of(2L, 4L, 11L, 9L)));
                assertTrue(count.isSuccess(), count.getErrorMessage());
                assertEquals(2, count.getResultValue());
                assertEquals(3, coordinator.getPersistentJac());
                assertTrue(a.ping()); assertTrue(b.ping());
            }
    }

    private static WorkerGrpcClient client(int id, int port) {
        WorkerIdentity identity = WorkerIdentity.newBuilder().setWorkerId(id).setSessionId("session-" + id).build();
        return new WorkerGrpcClient(WorkerDescriptor.newBuilder().setIdentity(identity)
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(port)).build(), ignored -> identity);
    }

    public static class RemoteCoordinator {
        public static void main(String[] args) throws Exception {
            verifyJobs(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
            System.out.println("PASS: MAX=11, PRIMESUM=17, PRIMECOUNT=2; progress, results and JAC verified");
        }
    }

    public static class RemoteWorker {
        public static void main(String[] args) throws Exception {
            WorkerGrpcServer server = new WorkerGrpcServer(Integer.parseInt(args[0]), 0, 2);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try { server.stop(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }));
            server.start();
            System.out.println("PORT=" + server.getPort());
            server.blockUntilShutdown();
        }
    }
}
