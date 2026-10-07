package worker.coordinator;

import common.interfaces.CoordinatorService;
import common.models.CoordinatorMessage;
import common.models.JobRequest;
import common.models.JobResult;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.atomic.AtomicInteger;

public class CoordinatorManager extends UnicastRemoteObject implements CoordinatorService {

    private static final long serialVersionUID = 1L;

    public static final int MAX_JOBS_PER_TERM = 5;

    private final AtomicInteger termJobCount = new AtomicInteger(0);

    private final java.util.concurrent.atomic.AtomicBoolean expiryNotified =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    private final AtomicInteger persistentJac;

    private final AtomicInteger currentTerm = new AtomicInteger(1);

    public CoordinatorManager(int initialJac) throws RemoteException {
        super();
        this.persistentJac = new AtomicInteger(initialJac);
    }

    private Runnable termExpiredListener;

    public void setTermExpiredListener(Runnable listener) {
        this.termExpiredListener = listener;
    }

    private void notifyTermExpired() {
        if (expiryNotified.compareAndSet(false, true) && termExpiredListener != null) {
            new Thread(termExpiredListener, "term-expiry-trigger").start();
        }
    }

    private final java.util.List<common.interfaces.WorkerService> activeWorkers = new java.util.concurrent.CopyOnWriteArrayList<>();

    private final java.util.concurrent.ExecutorService dispatchPool = java.util.concurrent.Executors.newCachedThreadPool();

    public void addWorker(common.interfaces.WorkerService worker) {
        if (worker != null && !activeWorkers.contains(worker)) {
            activeWorkers.add(worker);
        }
    }

    public void removeWorker(common.interfaces.WorkerService worker) {
        activeWorkers.remove(worker);
    }

    public java.util.List<common.interfaces.WorkerService> getActiveWorkers() {
        return activeWorkers;
    }

    @Override
    public JobResult submitJob(JobRequest request) throws RemoteException {
        long startTime = System.currentTimeMillis();
        JobResult finalResult = new JobResult();

        if (request == null) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("JobRequest cannot be null");
            finalResult.setExecutionTimeMs(System.currentTimeMillis() - startTime);
            return finalResult;
        }

        finalResult.setJobId(request.getJobId());

        if (isTermExpired()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("Coordinator term expired (5 jobs limit reached). Election required.");
            stepDown();
            notifyTermExpired();
            return finalResult;
        }

        if (activeWorkers.isEmpty()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("No active workers available in cluster to process job");
            return finalResult;
        }

        int numWorkers = activeWorkers.size();
        java.util.List<common.models.JobTask> tasks = new java.util.ArrayList<>();

        switch (request.getJobType()) {
            case MAX:
            case PRIMECOUNT:
                java.util.List<java.util.List<Long>> chunks = partitionList(request.getNumbers(), numWorkers);
                for (int i = 0; i < chunks.size(); i++) {
                    String taskId = request.getJobId() + "-task-" + (i + 1);
                    tasks.add(new common.models.JobTask(request.getJobId(), taskId, request.getJobType(), chunks.get(i)));
                }
                break;
            case PRIMESUM:
                java.util.List<long[]> ranges = partitionRange(request.getRangeStart(), request.getRangeEnd(), numWorkers);
                for (int i = 0; i < ranges.size(); i++) {
                    String taskId = request.getJobId() + "-task-" + (i + 1);
                    tasks.add(new common.models.JobTask(request.getJobId(), taskId, request.getJobType(), ranges.get(i)[0], ranges.get(i)[1]));
                }
                break;
            default:
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Unsupported job type: " + request.getJobType());
                return finalResult;
        }

        java.util.List<java.util.concurrent.Future<JobResult>> futures = new java.util.ArrayList<>();
        int workersAssignedCount = 0;

        for (int i = 0; i < tasks.size(); i++) {
            common.models.JobTask task = tasks.get(i);
            common.interfaces.WorkerService worker = activeWorkers.get(i % numWorkers);
            workersAssignedCount++;

            futures.add(dispatchPool.submit(() -> {
                try {
                    return worker.executeJob(task);
                } catch (Exception e) {
                    JobResult err = new JobResult();
                    err.setJobId(task.getJobId());
                    err.setTaskId(task.getTaskId());
                    err.setSuccess(false);
                    err.setErrorMessage("Worker RPC failed: " + e.getMessage());
                    return err;
                }
            }));
        }

        java.util.List<JobResult> subResults = new java.util.ArrayList<>();
        for (java.util.concurrent.Future<JobResult> future : futures) {
            try {
                JobResult res = future.get();
                if (res != null && !res.isSuccess()) {
                    finalResult.setSuccess(false);
                    finalResult.setErrorMessage("Subtask failure: " + res.getErrorMessage());
                    return finalResult;
                }
                subResults.add(res);
            } catch (Exception e) {
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Execution interrupted or failed: " + e.getMessage());
                return finalResult;
            }
        }

        aggregateResults(request.getJobType(), subResults, finalResult);

        if (workersAssignedCount > 0) {
            persistentJac.incrementAndGet();
        }
        termJobCount.incrementAndGet();

        finalResult.setExecutionTimeMs(System.currentTimeMillis() - startTime);

        if (isTermExpired()) {
            stepDown();
            notifyTermExpired();
        }

        return finalResult;
    }

    private java.util.List<java.util.List<Long>> partitionList(java.util.List<Long> numbers, int numChunks) {
        java.util.List<java.util.List<Long>> partitions = new java.util.ArrayList<>();
        if (numbers == null || numbers.isEmpty() || numChunks <= 0) {
            partitions.add(numbers != null ? numbers : new java.util.ArrayList<>());
            return partitions;
        }

        int totalSize = numbers.size();
        int actualChunks = Math.min(numChunks, totalSize);
        int baseChunkSize = totalSize / actualChunks;
        int remainder = totalSize % actualChunks;

        int currentIndex = 0;
        for (int i = 0; i < actualChunks; i++) {
            int currentSize = baseChunkSize + (i < remainder ? 1 : 0);
            partitions.add(new java.util.ArrayList<>(numbers.subList(currentIndex, currentIndex + currentSize)));
            currentIndex += currentSize;
        }
        return partitions;
    }

    private java.util.List<long[]> partitionRange(long start, long end, int numWorkers) {
        java.util.List<long[]> ranges = new java.util.ArrayList<>();
        if (start > end) {
            long temp = start;
            start = end;
            end = temp;
        }

        long totalElements = (end - start) + 1;
        int actualChunks = (int) Math.min(numWorkers, totalElements);
        long baseSize = totalElements / actualChunks;
        long remainder = totalElements % actualChunks;

        long currentStart = start;
        for (int i = 0; i < actualChunks; i++) {
            long currentChunkSize = baseSize + (i < remainder ? 1 : 0);
            long currentEnd = currentStart + currentChunkSize - 1;
            ranges.add(new long[]{currentStart, currentEnd});
            currentStart = currentEnd + 1;
        }
        return ranges;
    }

    private void aggregateResults(common.models.JobType jobType, java.util.List<JobResult> subResults, JobResult finalResult) {
        if (subResults.isEmpty()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("No subtask results to aggregate");
            return;
        }

        switch (jobType) {
            case MAX:
                long globalMax = Long.MIN_VALUE;
                for (JobResult r : subResults) {
                    if (r != null && r.getResultValue() > globalMax) {
                        globalMax = r.getResultValue();
                    }
                }
                finalResult.setSuccess(true);
                finalResult.setResultValue(globalMax);
                break;
            case PRIMESUM:
                long totalPrimeSum = 0;
                for (JobResult r : subResults) {
                    if (r != null) {
                        totalPrimeSum += r.getResultValue();
                    }
                }
                finalResult.setSuccess(true);
                finalResult.setResultValue(totalPrimeSum);
                break;
            case PRIMECOUNT:
                long totalPrimeCount = 0;
                for (JobResult r : subResults) {
                    if (r != null) {
                        totalPrimeCount += r.getResultValue();
                    }
                }
                finalResult.setSuccess(true);
                finalResult.setResultValue(totalPrimeCount);
                break;
            default:
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Unknown job type: " + jobType);
                break;
        }
    }

    @Override
    public void handleCoordinatorMessage(CoordinatorMessage message) throws RemoteException {

    }

   @Override
   public String getCoordinatorStatus()
        throws RemoteException {

    return "Term=" + currentTerm.get()
            + ", JobsAssigned="
            + termJobCount.get()
            + "/5"
            + ", JAC="
            + persistentJac.get();
    }

    public boolean isTermExpired() {
        return termJobCount.get() >= MAX_JOBS_PER_TERM;
    }

   public void stepDown() {

    System.out.println(
            "Coordinator term has ended after "
            + MAX_JOBS_PER_TERM
            + " jobs."
    );
    }

    public int getPersistentJac() {
        return persistentJac.get();
    }
}