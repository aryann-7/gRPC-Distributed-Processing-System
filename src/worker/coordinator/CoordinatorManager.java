package worker.coordinator;

import common.interfaces.CoordinatorService;
import common.models.CoordinatorMessage;
import common.models.JobRequest;
import common.models.JobResult;
import worker.recovery.WorkerFailureDetector.Health;
import java.util.concurrent.atomic.AtomicInteger;

public class CoordinatorManager implements CoordinatorService, AutoCloseable {

    public static final int MAX_JOBS_PER_TERM = 5;

    private final AtomicInteger termJobCount = new AtomicInteger(0);

    private final java.util.concurrent.atomic.AtomicBoolean expiryNotified =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    private final AtomicInteger persistentJac;

    private final AtomicInteger currentTerm = new AtomicInteger(1);

    private final int coordinatorId;
    private final worker.recovery.CoordinatorStateStore stateStore;
    private final worker.recovery.TaskLedger taskLedger;
    private final worker.recovery.WorkerFailureDetector failureDetector;

    public CoordinatorManager(int initialJac) {
        this(0, initialJac, 1, null, null);
    }

    public CoordinatorManager(int coordinatorId, int initialJac, int term) {
        this(coordinatorId, initialJac, term, null, null);
    }

    public CoordinatorManager(int coordinatorId, int initialJac, int term,
                              worker.recovery.TaskLedger ledger,
                              worker.recovery.WorkerFailureDetector detector) {
        this.coordinatorId = coordinatorId;
        this.persistentJac = new AtomicInteger(initialJac);
        this.currentTerm.set(term);

        if (detector != null) {
            this.failureDetector = detector;
        } else {
            this.failureDetector = new worker.recovery.WorkerFailureDetector(2000, 2);
        }

        if (ledger != null) {
            this.stateStore = null;
            this.taskLedger = ledger;
        } else {
            java.nio.file.Path statePath = java.nio.file.Paths.get("data", "coordinator-" + coordinatorId + "-state.bin");
            this.stateStore = new worker.recovery.CoordinatorStateStore(statePath);
            this.taskLedger = new worker.recovery.TaskLedger(stateStore, coordinatorId, term, initialJac);
        }
    }

    public int getCoordinatorId() {
        return coordinatorId;
    }

    public worker.recovery.TaskLedger getTaskLedger() {
        return taskLedger;
    }

    public worker.recovery.WorkerFailureDetector getFailureDetector() {
        return failureDetector;
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
        if (worker == null) {
            return;
        }
        int newId = getWorkerId(worker, -1);
        for (common.interfaces.WorkerService existing : activeWorkers) {
            if (getWorkerId(existing, -2) == newId) {
                return;
            }
        }
        activeWorkers.add(worker);
        System.out.println("Coordinator (term " + currentTerm + ") added Worker " + newId
                + " to active worker pool. Total active workers: " + activeWorkers.size());
    }

    public void removeWorker(common.interfaces.WorkerService worker) {
        activeWorkers.remove(worker);
    }

    public java.util.List<common.interfaces.WorkerService> getActiveWorkers() {
        return activeWorkers;
    }

    @Override
    public JobResult submitJob(JobRequest request) {
        return submitJob(request, (worker, update) -> {});
    }

    public JobResult submitJob(JobRequest request,
            java.util.function.BiConsumer<common.grpc.WorkerIdentity, common.grpc.TaskProgressUpdate> progress) {
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
        System.out.println("Coordinator: Partitioning job " + request.getJobId() + " (" + request.getJobType()
                + ") across " + numWorkers + " active workers: "
                + activeWorkers.stream().map(w -> getWorkerId(w, -1)).toList());
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

        // Register tasks in TaskLedger
        for (common.models.JobTask task : tasks) {
            try {
                taskLedger.addTask(task.getJobId(), task.getTaskId());
            } catch (Exception ignored) {
            }
        }

        java.util.List<JobResult> subResults = new java.util.concurrent.CopyOnWriteArrayList<>();
        java.util.concurrent.atomic.AtomicInteger workersAssignedCounter = new java.util.concurrent.atomic.AtomicInteger(0);
        java.util.concurrent.ConcurrentLinkedQueue<common.models.JobTask> pendingQueue =
                new java.util.concurrent.ConcurrentLinkedQueue<>(tasks);

        java.util.concurrent.atomic.AtomicInteger nextWorkerIdx = new java.util.concurrent.atomic.AtomicInteger(0);

        // Process all tasks, retrying / reassigning on failure to surviving workers
        java.util.List<java.util.concurrent.Future<?>> dispatchFutures = new java.util.ArrayList<>();

        // We launch initial task dispatches
        int taskCount = tasks.size();
        java.util.concurrent.CountDownLatch completionLatch = new java.util.concurrent.CountDownLatch(taskCount);
        java.util.concurrent.atomic.AtomicBoolean fatalError = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.atomic.AtomicReference<String> fatalMessage = new java.util.concurrent.atomic.AtomicReference<>(null);

        Runnable dispatchNextTask = new Runnable() {
            @Override
            public void run() {
                common.models.JobTask currentTask = pendingQueue.poll();
                if (currentTask == null) {
                    return;
                }

                // Pick an active worker that is healthy
                java.util.List<common.interfaces.WorkerService> workers = new java.util.ArrayList<>(activeWorkers);
                if (workers.isEmpty()) {
                    fatalError.set(true);
                    fatalMessage.set("No active workers remaining to process task " + currentTask.getTaskId());
                    while (completionLatch.getCount() > 0) {
                        completionLatch.countDown();
                    }
                    return;
                }

                int chosenIdx = Math.abs(nextWorkerIdx.getAndIncrement() % workers.size());
                common.interfaces.WorkerService worker = workers.get(chosenIdx);
                int workerId = getWorkerId(worker, chosenIdx);

                try {
                    taskLedger.assignTask(currentTask.getTaskId(), workerId);
                } catch (Exception ignored) {
                }
                workersAssignedCounter.incrementAndGet();

                long rpcStart = System.currentTimeMillis();
                try {
                    JobResult res;
                    if (worker instanceof worker.grpc.WorkerGrpcClient client) {
                        res = client.executeJob(currentTask, update -> progress.accept(client.descriptor().getIdentity(), update));
                    } else if (worker instanceof worker.WorkerNode node) {
                        common.grpc.WorkerIdentity wid = common.grpc.WorkerIdentity.newBuilder()
                                .setWorkerId(workerId)
                                .setSessionId("worker-" + workerId)
                                .build();
                        res = node.executeJob(currentTask, update -> progress.accept(wid, update));
                    } else {
                        res = worker.executeJob(currentTask);
                    }

                    long rpcDuration = System.currentTimeMillis() - rpcStart;

                    if (res != null && res.isSuccess()) {
                        long taskUnits;
                        if (currentTask.getJobType() == common.models.JobType.PRIMESUM) {
                            taskUnits = Math.abs(currentTask.getRangeEnd() - currentTask.getRangeStart()) + 1;
                        } else {
                            taskUnits = currentTask.getDataChunk() != null ? currentTask.getDataChunk().size() : 0;
                        }

                        common.grpc.WorkerIdentity wid;
                        if (worker instanceof worker.grpc.WorkerGrpcClient client) {
                            wid = client.descriptor().getIdentity();
                        } else {
                            wid = common.grpc.WorkerIdentity.newBuilder()
                                    .setWorkerId(workerId)
                                    .setSessionId("worker-" + workerId)
                                    .build();
                        }
                        common.grpc.TaskProgressUpdate compUpdate = common.grpc.TaskProgressUpdate.newBuilder()
                                .setJobId(currentTask.getJobId())
                                .setTaskId(currentTask.getTaskId())
                                .setWorkerId(workerId)
                                .setProgressPercentage(100.0)
                                .setUnitsCompleted(taskUnits)
                                .setTotalUnits(taskUnits)
                                .setStatusMessage("COMPLETED")
                                .build();
                        try {
                            progress.accept(wid, compUpdate);
                        } catch (Exception ignored) {
                        }

                        failureDetector.recordHeartbeat(workerId, rpcDuration);
                        try {
                            taskLedger.completeTask(currentTask.getTaskId(), workerId, res.getResultValue());
                        } catch (Exception ignored) {
                        }
                        subResults.add(res);
                        completionLatch.countDown();
                    } else {
                        // Worker returned failure or error
                        failureDetector.recordMissedHeartbeat(workerId);
                        handleTaskFailure(currentTask, worker, workerId, res != null ? res.getErrorMessage() : "RPC failed", this);
                    }
                } catch (Exception e) {
                    // Worker timed out or threw network exception
                    failureDetector.recordMissedHeartbeat(workerId);
                    handleTaskFailure(currentTask, worker, workerId, e.getMessage(), this);
                }
            }

            private void handleTaskFailure(common.models.JobTask currentTask,
                                           common.interfaces.WorkerService worker,
                                           int workerId,
                                           String reason,
                                           Runnable runner) {
                System.err.println("Task " + currentTask.getTaskId() + " failed on worker " + workerId
                        + " (" + reason + "). Reassigning to surviving worker...");
                try {
                    taskLedger.requeueWorkerTasks(workerId);
                } catch (Exception ignored) {
                }

                if (failureDetector.getHealth(workerId) == Health.SUSPECTED_FAILED) {
                    removeWorker(worker);
                }

                // Check surviving workers
                if (activeWorkers.isEmpty()) {
                    fatalError.set(true);
                    fatalMessage.set("All workers failed; could not complete " + currentTask.getTaskId()
                            + (reason != null ? " (" + reason + ")" : ""));
                    while (completionLatch.getCount() > 0) {
                        completionLatch.countDown();
                    }
                    return;
                }

                // Re-enqueue task and submit to dispatch pool
                pendingQueue.offer(currentTask);
                dispatchPool.submit(runner);
            }
        };

        for (int i = 0; i < taskCount; i++) {
            dispatchFutures.add(dispatchPool.submit(dispatchNextTask));
        }

        try {
            boolean done = completionLatch.await(10, java.util.concurrent.TimeUnit.MINUTES);
            if (!done) {
                finalResult.setSuccess(false);
                finalResult.setErrorMessage("Timed out waiting for subtasks to finish");
                return finalResult;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            finalResult.setSuccess(false);
            finalResult.setErrorMessage("Coordinator execution interrupted: " + e.getMessage());
            return finalResult;
        }

        if (fatalError.get()) {
            finalResult.setSuccess(false);
            finalResult.setErrorMessage(fatalMessage.get());
            return finalResult;
        }

        aggregateResults(request.getJobType(), new java.util.ArrayList<>(subResults), finalResult);

        if (workersAssignedCounter.get() > 0) {
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

    private int getWorkerId(common.interfaces.WorkerService worker, int fallbackIdx) {
        if (worker instanceof worker.grpc.WorkerGrpcClient client) {
            return client.descriptor().getIdentity().getWorkerId();
        }
        if (worker instanceof worker.WorkerNode node) {
            return node.getWorkerId();
        }
        try {
            java.lang.reflect.Method m = worker.getClass().getMethod("getWorkerId");
            return (Integer) m.invoke(worker);
        } catch (Exception ignored) {
        }
        return fallbackIdx;
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
    public void handleCoordinatorMessage(CoordinatorMessage message) {

    }

   @Override
   public String getCoordinatorStatus() {

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

    public int getCurrentTerm() {
        return currentTerm.get();
    }

    public void setCurrentTerm(int term) {
        this.currentTerm.set(term);
    }

    public int getTermJobCount() {
        return termJobCount.get();
    }

    @Override
    public void close() {
        dispatchPool.shutdownNow();
    }
}

