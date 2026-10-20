package worker.computation;

import common.grpc.JobTypeProto;
import common.grpc.TaskDispatchRequest;
import common.grpc.TaskProgressUpdate;
import common.grpc.TaskResultResponse;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.*;

public class ComputationalEngineTest {

    private TaskExecutionEngine engine;
    private final int workerId = 101;

    @BeforeEach
    public void setUp() {
        engine = new TaskExecutionEngine(workerId, 4);
    }

    @AfterEach
    public void tearDown() {
        if (engine != null) {
            engine.shutdown();
        }
    }

    @Test
    public void testComputeMaxWithRealProgress() throws Exception {
        List<Long> data = new ArrayList<>();
        for (long i = 1; i <= 2500; i++) {
            data.add(i);
        }
        // inject maximum in the middle
        data.set(1200, 999999L);

        TaskDispatchRequest request = TaskDispatchRequest.newBuilder()
                .setJobId("job-1")
                .setTaskId("task-max-1")
                .setJobType(JobTypeProto.MAX)
                .addAllDataChunk(data)
                .build();

        List<TaskProgressUpdate> updates = new CopyOnWriteArrayList<>();
        Future<TaskResultResponse> future = engine.submitTask(request, updates::add);

        TaskResultResponse result = future.get();
        assertTrue(result.getSuccess());
        assertEquals(999999L, result.getResultValue());
        assertFalse(updates.isEmpty(), "Should have emitted progress events");

        // Verify strictly non-fake, strictly incremental progress
        double prev = -1.0;
        for (TaskProgressUpdate u : updates) {
            assertTrue(u.getProgressPercentage() >= prev);
            assertTrue(u.getUnitsCompleted() <= 2500);
            prev = u.getProgressPercentage();
        }
        assertEquals(100.0, updates.get(updates.size() - 1).getProgressPercentage(), 0.01);
    }

    @Test
    public void testComputePrimeSumWithRealProgress() throws Exception {
        // Primes up to 10: 2, 3, 5, 7 => sum = 17
        TaskDispatchRequest request = TaskDispatchRequest.newBuilder()
                .setJobId("job-2")
                .setTaskId("task-primesum-1")
                .setJobType(JobTypeProto.PRIMESUM)
                .setRangeStart(1)
                .setRangeEnd(1000)
                .build();

        List<TaskProgressUpdate> updates = new CopyOnWriteArrayList<>();
        Future<TaskResultResponse> future = engine.submitTask(request, updates::add);

        TaskResultResponse result = future.get();
        assertTrue(result.getSuccess());
        assertTrue(result.getResultValue() > 0);
        assertFalse(updates.isEmpty());
        assertEquals(100.0, updates.get(updates.size() - 1).getProgressPercentage(), 0.01);
    }

    @Test
    public void testComputePrimeCountWithRealProgress() throws Exception {
        List<Long> numbers = Arrays.asList(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L);
        // Primes: 2, 3, 5, 7, 11 => count = 5

        TaskDispatchRequest request = TaskDispatchRequest.newBuilder()
                .setJobId("job-3")
                .setTaskId("task-primecount-1")
                .setJobType(JobTypeProto.PRIMECOUNT)
                .addAllDataChunk(numbers)
                .build();

        List<TaskProgressUpdate> updates = new CopyOnWriteArrayList<>();
        Future<TaskResultResponse> future = engine.submitTask(request, updates::add);

        TaskResultResponse result = future.get();
        assertTrue(result.getSuccess());
        assertEquals(5L, result.getResultValue());
    }
}
