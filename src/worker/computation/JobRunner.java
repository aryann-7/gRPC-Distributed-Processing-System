package worker.computation;

import common.models.JobResult;
import common.models.JobTask;
import java.util.concurrent.Callable;

public class JobRunner implements Callable<JobResult> {
    private final JobTask task;

    public JobRunner(JobTask task) {
        this.task = task;
    }

    // Executes the task on a Java thread and returns the computed result.
    @Override
    public JobResult call() throws Exception {
        long startTime = System.currentTimeMillis();
        JobResult result = new JobResult();
        result.setJobId(task.getJobId());
        result.setTaskId(task.getTaskId());

        try {
            if (task.getJobType() == null) {
                result.setSuccess(false);
                result.setErrorMessage("JobType cannot be null");
                result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
                return result;
            }

            switch (task.getJobType()) {
                case MAX:
                    result = computeMax();
                    break;
                case PRIMESUM:
                    result = computePrimeSum();
                    break;
                case PRIMECOUNT:
                    result = computePrimeCount();
                    break;
                default:
                    result.setSuccess(false);
                    result.setErrorMessage("Unsupported JobType: " + task.getJobType());
                    break;
            }
        } catch (Exception e) {
            result.setSuccess(false);
            result.setErrorMessage("Execution error: " + e.getMessage());
        }

        result.setJobId(task.getJobId());
        result.setTaskId(task.getTaskId());
        result.setExecutionTimeMs(System.currentTimeMillis() - startTime);
        return result;
    }

    // Computes the maximum value from an unsorted list of numbers.
    private JobResult computeMax() {
        JobResult result = new JobResult();
        if (task.getDataChunk() == null || task.getDataChunk().isEmpty()) {
            result.setSuccess(false);
            result.setErrorMessage("Data chunk is empty or null for MAX task");
            return result;
        }

        long max = Long.MIN_VALUE;
        for (Long num : task.getDataChunk()) {
            if (num != null && num > max) {
                max = num;
            }
        }

        result.setSuccess(true);
        result.setResultValue(max);
        return result;
    }

    // Computes the sum of prime numbers within a specified range [start, end].
    private JobResult computePrimeSum() {
        JobResult result = new JobResult();
        long start = task.getRangeStart();
        long end = task.getRangeEnd();

        if (start > end) {
            long temp = start;
            start = end;
            end = temp;
        }

        long sum = 0;
        for (long i = start; i <= end; i++) {
            if (isPrime(i)) {
                sum += i;
            }
        }

        result.setSuccess(true);
        result.setResultValue(sum);
        return result;
    }

    // Computes the total count of prime numbers in an unsorted list of numbers.
    private JobResult computePrimeCount() {
        JobResult result = new JobResult();
        if (task.getDataChunk() == null) {
            result.setSuccess(false);
            result.setErrorMessage("Data chunk is null for PRIMECOUNT task");
            return result;
        }

        long count = 0;
        for (Long num : task.getDataChunk()) {
            if (num != null && isPrime(num)) {
                count++;
            }
        }

        result.setSuccess(true);
        result.setResultValue(count);
        return result;
    }

    // Helper to verify if a number is prime.
    private boolean isPrime(long n) {
        if (n <= 1) {
            return false;
        }
        if (n <= 3) {
            return true;
        }
        if (n % 2 == 0 || n % 3 == 0) {
            return false;
        }
        for (long i = 5; i * i <= n; i += 6) {
            if (n % i == 0 || n % (i + 2) == 0) {
                return false;
            }
        }
        return true;
    }
}