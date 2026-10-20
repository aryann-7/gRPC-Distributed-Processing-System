package worker.computation;

import common.grpc.JobTypeProto;
import common.grpc.TaskDispatchRequest;
import common.grpc.TaskProgressUpdate;
import common.grpc.TaskResultResponse;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Executes a computational sub-task on a native Java worker thread.
 * Emits fine-grained, real-time progress updates based strictly on the
 * physical volume of data scanned or numeric range traversed.
 *
 * Artificial timers, sleep calls, or mock progress loops are strictly avoided.
 */
public class GranularJobRunner implements Callable<TaskResultResponse> {

    private final TaskDispatchRequest request;
    private final int workerId;
    private final ProgressListener progressListener;
    private final AtomicBoolean cancelled = new AtomicBoolean(false);
    private final java.util.concurrent.atomic.AtomicLong lastProcessedUnits = new java.util.concurrent.atomic.AtomicLong(
            0);
    private final java.util.concurrent.atomic.AtomicLong lastTotalUnits = new java.util.concurrent.atomic.AtomicLong(0);

    public GranularJobRunner(TaskDispatchRequest request, int workerId, ProgressListener progressListener) {
        this.request = request;
        this.workerId = workerId;
        this.progressListener = progressListener;
    }

    public void cancel() {
        cancelled.set(true);
    }

    public boolean isCancelled() {
        return cancelled.get();
    }

    public long getLastProcessedUnits() {
        return lastProcessedUnits.get();
    }

    public long getLastTotalUnits() {
        return lastTotalUnits.get();
    }

    @Override
    public TaskResultResponse call() {
        long startTime = System.currentTimeMillis();
        String jobId = request.getJobId();
        String taskId = request.getTaskId();
        JobTypeProto jobType = request.getJobType();

        try {
            if (jobType == null || jobType == JobTypeProto.JOB_TYPE_UNSPECIFIED) {
                return buildErrorResponse(jobId, taskId, "Unsupported or unspecified JobType", startTime);
            }

            switch (jobType) {
                case MAX:
                    return computeMax(startTime);
                case PRIMESUM:
                    return computePrimeSum(startTime);
                case PRIMECOUNT:
                    return computePrimeCount(startTime);
                default:
                    return buildErrorResponse(jobId, taskId, "Unknown JobType: " + jobType, startTime);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            emitProgress(lastProcessedUnits.get(), lastTotalUnits.get(), "CANCELLED");
            return buildErrorResponse(jobId, taskId, "Task interrupted/cancelled: " + e.getMessage(), startTime);
        } catch (Exception e) {
            return buildErrorResponse(jobId, taskId, "Execution error: " + e.getMessage(), startTime);
        }
    }

    /**
     * MAX: Iterates over the data chunk in batches, calculating running max
     * and emitting true progress percentage based on array index reached.
     */
    private TaskResultResponse computeMax(long startTime) throws InterruptedException {
        List<Long> data = request.getDataChunkList();
        int totalUnits = data.size();

        if (totalUnits == 0) {
            return buildErrorResponse(request.getJobId(), request.getTaskId(), "Data chunk is empty for MAX task",
                    startTime);
        }

        long max = Long.MIN_VALUE;
        long lastEmitUnits = 0;
        long emitInterval = Math.max(50L, Math.min(10000L, (long) totalUnits / 200));
        long lastEmitTime = System.currentTimeMillis();

        emitProgress(0, totalUnits, "PROCESSING");

        for (int i = 0; i < totalUnits; i++) {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("MAX computation was cancelled");
            }

            Long num = data.get(i);
            if (num != null && num > max) {
                max = num;
            }

            int processed = i + 1;
            long now = System.currentTimeMillis();
            if (processed - lastEmitUnits >= emitInterval || (now - lastEmitTime >= 100 && processed > lastEmitUnits)
                    || processed == totalUnits) {
                emitProgress(processed, totalUnits, "PROCESSING");
                lastEmitUnits = processed;
                lastEmitTime = now;
            }
        }

        emitProgress(totalUnits, totalUnits, "COMPLETED");

        return TaskResultResponse.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getTaskId())
                .setWorkerId(workerId)
                .setSuccess(true)
                .setResultValue(max)
                .setExecutionTimeMs(System.currentTimeMillis() - startTime)
                .build();
    }

    /**
     * PRIMESUM: Evaluates prime numbers within [start, end].
     * Emits real progress based on (current - start + 1) / totalRange.
     */
    private TaskResultResponse computePrimeSum(long startTime) throws InterruptedException {
        long start = request.getRangeStart();
        long end = request.getRangeEnd();

        if (start > end) {
            long temp = start;
            start = end;
            end = temp;
        }

        long totalUnits = (end - start) + 1;
        if (totalUnits <= 0) {
            return buildErrorResponse(request.getJobId(), request.getTaskId(), "Invalid range for PRIMESUM", startTime);
        }

        long sum = 0;
        long processed = 0;
        long lastEmitUnits = 0;
        long emitInterval = Math.max(50L, Math.min(10000L, totalUnits / 200));
        long lastEmitTime = System.currentTimeMillis();

        emitProgress(0, totalUnits, "PROCESSING");

        // Fast-path handling: 2 is the only even prime
        if (start <= 2 && 2 <= end) {
            sum += 2;
        }

        // Adjust loop start to first odd number >= Math.max(3, start)
        long firstOdd = Math.max(3, start);
        if (firstOdd % 2 == 0) {
            firstOdd++;
        }

        for (long curr = firstOdd; curr <= end; curr += 2) {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("PRIMESUM computation was cancelled");
            }

            if (isPrime(curr)) {
                sum += curr;
            }

            // Estimate/track progress
            processed = (curr - start) + 1;
            long now = System.currentTimeMillis();
            if (processed - lastEmitUnits >= emitInterval || (now - lastEmitTime >= 100 && processed > lastEmitUnits)
                    || curr >= end - 1) {
                emitProgress(Math.min(processed, totalUnits), totalUnits, "PROCESSING");
                lastEmitUnits = processed;
                lastEmitTime = now;
            }
        }

        emitProgress(totalUnits, totalUnits, "COMPLETED");

        return TaskResultResponse.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getTaskId())
                .setWorkerId(workerId)
                .setSuccess(true)
                .setResultValue(sum)
                .setExecutionTimeMs(System.currentTimeMillis() - startTime)
                .build();
    }

    /**
     * PRIMECOUNT: Counts prime numbers in data chunk.
     * Emits real progress based on number of elements tested.
     */
    private TaskResultResponse computePrimeCount(long startTime) throws InterruptedException {
        List<Long> data = request.getDataChunkList();
        int totalUnits = data.size();

        if (totalUnits == 0) {
            return buildErrorResponse(request.getJobId(), request.getTaskId(),
                    "Data chunk is empty for PRIMECOUNT task", startTime);
        }

        long count = 0;
        long lastEmitUnits = 0;
        long emitInterval = Math.max(50L, Math.min(10000L, (long) totalUnits / 200));
        long lastEmitTime = System.currentTimeMillis();

        emitProgress(0, totalUnits, "PROCESSING");

        for (int i = 0; i < totalUnits; i++) {
            if (cancelled.get() || Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("PRIMECOUNT computation was cancelled");
            }

            Long num = data.get(i);
            if (num != null) {
                long val = num;
                if (val == 2 || (val > 2 && (val & 1) != 0 && isPrime(val))) {
                    count++;
                }
            }

            int processed = i + 1;
            long now = System.currentTimeMillis();
            if (processed - lastEmitUnits >= emitInterval || (now - lastEmitTime >= 100 && processed > lastEmitUnits)
                    || processed == totalUnits) {
                emitProgress(processed, totalUnits, "PROCESSING");
                lastEmitUnits = processed;
                lastEmitTime = now;
            }
        }

        emitProgress(totalUnits, totalUnits, "COMPLETED");

        return TaskResultResponse.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getTaskId())
                .setWorkerId(workerId)
                .setSuccess(true)
                .setResultValue(count)
                .setExecutionTimeMs(System.currentTimeMillis() - startTime)
                .build();
    }

    private void emitProgress(long processed, long total, String status) {
        if (total > 0) {
            lastProcessedUnits.set(processed);
            lastTotalUnits.set(total);
        }
        if (progressListener == null || total <= 0) {
            return;
        }

        double percentage = ((double) processed / total) * 100.0;
        if (percentage > 100.0)
            percentage = 100.0;

        TaskProgressUpdate update = TaskProgressUpdate.newBuilder()
                .setJobId(request.getJobId())
                .setTaskId(request.getTaskId())
                .setWorkerId(workerId)
                .setProgressPercentage(percentage)
                .setUnitsCompleted(processed)
                .setTotalUnits(total)
                .setStatusMessage(status)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        try {
            progressListener.onProgress(update);
        } catch (Exception e) {
            // Ignore callback transport errors if client disconnected
        }
    }

    private TaskResultResponse buildErrorResponse(String jobId, String taskId, String message, long startTime) {
        return TaskResultResponse.newBuilder()
                .setJobId(jobId)
                .setTaskId(taskId)
                .setWorkerId(workerId)
                .setSuccess(false)
                .setErrorMessage(message)
                .setExecutionTimeMs(System.currentTimeMillis() - startTime)
                .build();
    }

    private static final int SIEVE_LIMIT = 100_000;
    private static final boolean[] IS_PRIME = new boolean[SIEVE_LIMIT + 1];

    static {
        java.util.Arrays.fill(IS_PRIME, true);
        IS_PRIME[0] = false;
        if (SIEVE_LIMIT >= 1)
            IS_PRIME[1] = false;
        for (int p = 2; p * p <= SIEVE_LIMIT; p++) {
            if (IS_PRIME[p]) {
                for (int i = p * p; i <= SIEVE_LIMIT; i += p) {
                    IS_PRIME[i] = false;
                }
            }
        }
    }

    /**
     * O(1) lookup for numbers <= 100,000, fallback to 6k +/- 1 for larger numbers.
     */
    private boolean isPrime(long n) {
        if (n <= 1)
            return false;
        if (n <= SIEVE_LIMIT) {
            return IS_PRIME[(int) n];
        }
        if (n <= 3)
            return true;
        if (n % 2 == 0 || n % 3 == 0)
            return false;
        for (long i = 5; i * i <= n; i += 6) {
            if (n % i == 0 || n % (i + 2) == 0)
                return false;
        }
        return true;
    }
}
