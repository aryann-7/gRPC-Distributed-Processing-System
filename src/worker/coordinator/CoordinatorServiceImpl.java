package worker.coordinator;

import common.grpc.*;
import common.models.JobRequest;
import common.models.JobResult;
import common.models.JobType;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.Supplier;

/**
 * gRPC service implementing CoordinatorService defined in coordinator.proto.
 *
 * Exposes:
 * - SubmitJob: non-blocking job acceptance, async execution via
 * CoordinatorManager.
 * - WatchJob: server-streaming progress updates (QUEUED -> RUNNING ->
 * COMPLETED/FAILED).
 * - GetJobStatus: unary snapshot query for a given job ID.
 * - GetCoordinatorStatus: metadata query for current coordinator state and
 * terms.
 */
public class CoordinatorServiceImpl extends CoordinatorServiceGrpc.CoordinatorServiceImplBase implements AutoCloseable {

    private final Supplier<CoordinatorManager> coordinatorManagerSupplier;
    private final Supplier<Boolean> isCoordinatorSupplier;
    private final Supplier<WorkerIdentity> identitySupplier;
    private final Runnable workerRefresher;

    private final Map<String, JobRecord> jobs = new ConcurrentHashMap<>();
    private final ExecutorService jobExecutor = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "grpc-coordinator-job-runner");
        t.setDaemon(true);
        return t;
    });

    public CoordinatorServiceImpl(
            Supplier<CoordinatorManager> coordinatorManagerSupplier,
            Supplier<Boolean> isCoordinatorSupplier,
            Supplier<WorkerIdentity> identitySupplier) {
        this(coordinatorManagerSupplier, isCoordinatorSupplier, identitySupplier, null);
    }

    public CoordinatorServiceImpl(
            Supplier<CoordinatorManager> coordinatorManagerSupplier,
            Supplier<Boolean> isCoordinatorSupplier,
            Supplier<WorkerIdentity> identitySupplier,
            Runnable workerRefresher) {
        this.coordinatorManagerSupplier = coordinatorManagerSupplier != null
                ? coordinatorManagerSupplier
                : () -> null;
        this.isCoordinatorSupplier = isCoordinatorSupplier != null
                ? isCoordinatorSupplier
                : () -> false;
        this.identitySupplier = identitySupplier != null
                ? identitySupplier
                : () -> WorkerIdentity.getDefaultInstance();
        this.workerRefresher = workerRefresher;
    }

    public CoordinatorServiceImpl(
            Supplier<CoordinatorManager> coordinatorManagerSupplier,
            Supplier<Boolean> isCoordinatorSupplier,
            int workerId) {
        this(coordinatorManagerSupplier, isCoordinatorSupplier, () -> WorkerIdentity.newBuilder()
                .setWorkerId(workerId)
                .setSessionId("worker-" + workerId)
                .build(), null);
    }

    @Override
    public void submitJob(SubmitJobRequest request, StreamObserver<SubmitJobResponse> responseObserver) {
        if (request == null || request.getJobId() == null || request.getJobId().trim().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Job ID cannot be empty")
                    .asRuntimeException());
            return;
        }

        if (!Boolean.TRUE.equals(isCoordinatorSupplier.get()) || coordinatorManagerSupplier.get() == null) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Worker is not the active coordinator")
                    .asRuntimeException());
            return;
        }

        CoordinatorManager cm = coordinatorManagerSupplier.get();
        if (cm.isTermExpired()) {
            responseObserver.onError(Status.ABORTED
                    .withDescription("Coordinator term expired; election required")
                    .asRuntimeException());
            return;
        }

        JobTypeProto type = request.getJobType();
        if (type == JobTypeProto.JOB_TYPE_UNSPECIFIED || type == JobTypeProto.UNRECOGNIZED) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Unsupported or unspecified job type")
                    .asRuntimeException());
            return;
        }

        long totalUnits;
        if (type == JobTypeProto.MAX || type == JobTypeProto.PRIMECOUNT) {
            if (request.getInputCase() != SubmitJobRequest.InputCase.NUMBERS
                    || request.getNumbers().getNumbersCount() == 0) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("MAX and PRIMECOUNT require non-empty numbers list")
                        .asRuntimeException());
                return;
            }
            totalUnits = request.getNumbers().getNumbersCount();
        } else if (type == JobTypeProto.PRIMESUM) {
            if (request.getInputCase() != SubmitJobRequest.InputCase.RANGE) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                        .withDescription("PRIMESUM requires a range input")
                        .asRuntimeException());
                return;
            }
            totalUnits = Math.abs(request.getRange().getEnd() - request.getRange().getStart()) + 1;
        } else {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Unknown job type: " + type)
                    .asRuntimeException());
            return;
        }

        String jobId = request.getJobId();
        JobRecord newRecord = new JobRecord(jobId, request, totalUnits);
        JobRecord existing = jobs.putIfAbsent(jobId, newRecord);
        if (existing != null) {
            if (existing.matches(request)) {
                // Exact retry returns the existing ID without duplicate execution
                responseObserver.onNext(SubmitJobResponse.newBuilder().setJobId(jobId).build());
                responseObserver.onCompleted();
                return;
            } else {
                responseObserver.onError(Status.ALREADY_EXISTS
                        .withDescription("Job ID " + jobId + " already exists with different parameters")
                        .asRuntimeException());
                return;
            }
        }

        JobRecord record = newRecord;
        responseObserver.onNext(SubmitJobResponse.newBuilder().setJobId(jobId).build());
        responseObserver.onCompleted();

        jobExecutor.execute(() -> {
            if (workerRefresher != null) {
                try {
                    workerRefresher.run();
                } catch (Exception ignored) {
                }
            }
            record.state = JobStateProto.RUNNING;
            record.broadcast();
            try {
                JobRequest modelRequest;
                if (type == JobTypeProto.MAX) {
                    modelRequest = new JobRequest(jobId, JobType.MAX, request.getNumbers().getNumbersList());
                } else if (type == JobTypeProto.PRIMECOUNT) {
                    modelRequest = new JobRequest(jobId, JobType.PRIMECOUNT, request.getNumbers().getNumbersList());
                } else {
                    modelRequest = new JobRequest(jobId, JobType.PRIMESUM,
                            request.getRange().getStart(), request.getRange().getEnd());
                }

                JobResult result = cm.submitJob(modelRequest, record::onTaskProgress);
                if (result.isSuccess()) {
                    record.complete(result.getResultValue(), result.getExecutionTimeMs());
                } else {
                    record.fail(result.getErrorMessage(), result.getExecutionTimeMs());
                }
            } catch (Exception e) {
                record.fail("Coordinator execution error: " + e.getMessage(), 0);
            }
        });
    }

    @Override
    public void watchJob(WatchJobRequest request, StreamObserver<JobProgress> responseObserver) {
        if (request == null || request.getJobId() == null || request.getJobId().trim().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Job ID cannot be empty")
                    .asRuntimeException());
            return;
        }

        JobRecord record = jobs.get(request.getJobId());
        if (record == null) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Job not found: " + request.getJobId())
                    .asRuntimeException());
            return;
        }

        synchronized (record) {
            responseObserver.onNext(record.toProto());
            if (record.state == JobStateProto.COMPLETED || record.state == JobStateProto.FAILED) {
                responseObserver.onCompleted();
                return;
            }
            record.watchers.add(responseObserver);
        }

        if (responseObserver instanceof ServerCallStreamObserver<?> serverCallObserver) {
            serverCallObserver.setOnCancelHandler(() -> record.watchers.remove(responseObserver));
        }
    }

    @Override
    public void getJobStatus(GetJobStatusRequest request, StreamObserver<JobProgress> responseObserver) {
        if (request == null || request.getJobId() == null || request.getJobId().trim().isEmpty()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Job ID cannot be empty")
                    .asRuntimeException());
            return;
        }

        JobRecord record = jobs.get(request.getJobId());
        if (record == null) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Job not found: " + request.getJobId())
                    .asRuntimeException());
            return;
        }

        responseObserver.onNext(record.toProto());
        responseObserver.onCompleted();
    }

    @Override
    public void getCoordinatorStatus(GetCoordinatorStatusRequest request,
            StreamObserver<GetCoordinatorStatusResponse> responseObserver) {
        if (!Boolean.TRUE.equals(isCoordinatorSupplier.get()) || coordinatorManagerSupplier.get() == null) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Worker is not the active coordinator")
                    .asRuntimeException());
            return;
        }

        CoordinatorManager cm = coordinatorManagerSupplier.get();
        WorkerIdentity self = identitySupplier != null ? identitySupplier.get() : null;
        if (self == null) {
            self = WorkerIdentity.getDefaultInstance();
        }

        GetCoordinatorStatusResponse response = GetCoordinatorStatusResponse.newBuilder()
                .setCoordinator(self)
                .setTerm(cm.getCurrentTerm())
                .setJobAllocationCounter(cm.getPersistentJac())
                .setJobsInTerm(cm.getTermJobCount())
                .setMaxJobsPerTerm(CoordinatorManager.MAX_JOBS_PER_TERM)
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void close() {
        jobExecutor.shutdownNow();
    }

    private static class TaskProgress {
        final WorkerIdentity worker;
        final long unitsCompleted;
        final long totalUnits;

        TaskProgress(WorkerIdentity worker, long unitsCompleted, long totalUnits) {
            this.worker = worker;
            this.unitsCompleted = unitsCompleted;
            this.totalUnits = totalUnits;
        }
    }

    private class JobRecord {
        final String jobId;
        final SubmitJobRequest request;
        final long totalUnits;
        final long submitTimeMs;

        volatile JobStateProto state = JobStateProto.QUEUED;
        volatile long unitsCompleted = 0;
        volatile Long resultValue = null;
        volatile String errorMessage = null;
        volatile long executionTimeMs = 0;

        final Map<String, TaskProgress> taskProgressMap = new ConcurrentHashMap<>();
        final List<StreamObserver<JobProgress>> watchers = new CopyOnWriteArrayList<>();

        JobRecord(String jobId, SubmitJobRequest request, long totalUnits) {
            this.jobId = jobId;
            this.request = request;
            this.totalUnits = totalUnits;
            this.submitTimeMs = System.currentTimeMillis();
        }

        boolean matches(SubmitJobRequest other) {
            if (request.getJobType() != other.getJobType())
                return false;
            if (request.getInputCase() != other.getInputCase())
                return false;
            if (request.getInputCase() == SubmitJobRequest.InputCase.NUMBERS) {
                return request.getNumbers().getNumbersList().equals(other.getNumbers().getNumbersList());
            } else if (request.getInputCase() == SubmitJobRequest.InputCase.RANGE) {
                return request.getRange().getStart() == other.getRange().getStart()
                        && request.getRange().getEnd() == other.getRange().getEnd();
            }
            return true;
        }

        synchronized JobProgress toProto() {
            JobProgress.Builder b = JobProgress.newBuilder()
                    .setJobId(jobId)
                    .setState(state)
                    .setUnitsCompleted(unitsCompleted)
                    .setTotalUnits(totalUnits)
                    .setExecutionTimeMs(state == JobStateProto.COMPLETED || state == JobStateProto.FAILED
                            ? executionTimeMs
                            : (System.currentTimeMillis() - submitTimeMs));

            Map<Integer, WorkerJobProgress.Builder> workerAggregates = new java.util.LinkedHashMap<>();
            for (TaskProgress tp : taskProgressMap.values()) {
                int wId = tp.worker.getWorkerId();
                WorkerJobProgress.Builder existing = workerAggregates.get(wId);
                if (existing == null) {
                    workerAggregates.put(wId, WorkerJobProgress.newBuilder()
                            .setWorker(tp.worker)
                            .setUnitsCompleted(tp.unitsCompleted)
                            .setTotalUnits(tp.totalUnits));
                } else {
                    existing.setUnitsCompleted(existing.getUnitsCompleted() + tp.unitsCompleted);
                    existing.setTotalUnits(existing.getTotalUnits() + tp.totalUnits);
                }
            }

            for (WorkerJobProgress.Builder wb : workerAggregates.values()) {
                b.addWorkers(wb.build());
            }

            if (state == JobStateProto.COMPLETED && resultValue != null) {
                b.setResultValue(resultValue);
            } else if (state == JobStateProto.FAILED && errorMessage != null) {
                b.setErrorMessage(errorMessage);
            }
            return b.build();
        }

        synchronized void broadcast() {
            JobProgress progress = toProto();
            for (StreamObserver<JobProgress> watcher : watchers) {
                try {
                    watcher.onNext(progress);
                } catch (Exception e) {
                    watchers.remove(watcher);
                }
            }
        }

        synchronized void complete(long result, long execTime) {
            this.state = JobStateProto.COMPLETED;
            this.resultValue = result;
            this.executionTimeMs = execTime;
            this.unitsCompleted = totalUnits;

            // Ensure any reported tasks reflect 100% completion in the final snapshot
            for (Map.Entry<String, TaskProgress> entry : taskProgressMap.entrySet()) {
                TaskProgress tp = entry.getValue();
                if (tp.unitsCompleted < tp.totalUnits) {
                    entry.setValue(new TaskProgress(tp.worker, tp.totalUnits, tp.totalUnits));
                }
            }

            // Fallback: If no subtasks were tracked individually, attribute full completion
            // to self/coordinator
            if (taskProgressMap.isEmpty()) {
                WorkerIdentity self = identitySupplier != null ? identitySupplier.get() : null;
                if (self != null && self.getWorkerId() > 0) {
                    taskProgressMap.put(jobId + "-task-1", new TaskProgress(self, totalUnits, totalUnits));
                }
            }

            JobProgress progress = toProto();
            for (StreamObserver<JobProgress> watcher : watchers) {
                try {
                    watcher.onNext(progress);
                    watcher.onCompleted();
                } catch (Exception ignored) {
                }
            }
            watchers.clear();
        }

        synchronized void fail(String error, long execTime) {
            this.state = JobStateProto.FAILED;
            this.errorMessage = error != null ? error : "Job execution failed";
            this.executionTimeMs = execTime;
            JobProgress progress = toProto();
            for (StreamObserver<JobProgress> watcher : watchers) {
                try {
                    watcher.onNext(progress);
                    watcher.onCompleted();
                } catch (Exception ignored) {
                }
            }
            watchers.clear();
        }

        synchronized void onTaskProgress(WorkerIdentity worker, TaskProgressUpdate update) {
            String taskId = update.getTaskId() != null && !update.getTaskId().isEmpty()
                    ? update.getTaskId()
                    : ("task-worker-" + worker.getWorkerId());
            taskProgressMap.put(taskId, new TaskProgress(worker, update.getUnitsCompleted(), update.getTotalUnits()));

            long sumCompleted = taskProgressMap.values().stream()
                    .mapToLong(tp -> tp.unitsCompleted).sum();
            this.unitsCompleted = Math.min(totalUnits, sumCompleted);
            if (this.state == JobStateProto.QUEUED) {
                this.state = JobStateProto.RUNNING;
            }
            broadcast();
        }
    }
}
