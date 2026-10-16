package worker.grpc;

import common.grpc.*;
import io.grpc.stub.StreamObserver;
import worker.computation.ProgressListener;
import worker.computation.TaskExecutionEngine;

import java.util.concurrent.Future;

/**
 * Worker-side gRPC Service Implementation.
 * Extends the generated WorkerServiceGrpc.WorkerServiceImplBase.
 * Handles task dispatching, streaming progress reporting back to Coordinator,
 * liveness pings/heartbeats, and task preemption/cancellation.
 */
public class WorkerServiceImpl extends WorkerServiceGrpc.WorkerServiceImplBase {

    private final int workerId;
    private final TaskExecutionEngine executionEngine;
    private volatile int currentTerm = 1;

    public WorkerServiceImpl(int workerId, TaskExecutionEngine executionEngine) {
        this.workerId = workerId;
        this.executionEngine = executionEngine;
    }

    public void setCurrentTerm(int term) {
        this.currentTerm = term;
    }

    /**
     * Coordinator dispatches a sub-task; Worker streams back real-time progress events
     * as computation advances, strictly reflecting physical items evaluated.
     */
    @Override
    public void executeTaskStream(TaskDispatchRequest request, StreamObserver<TaskProgressUpdate> responseObserver) {
        java.util.concurrent.atomic.AtomicReference<TaskProgressUpdate> latest = new java.util.concurrent.atomic.AtomicReference<>();
        // Callback listener that forwards each incremental progress update through gRPC stream
        ProgressListener progressListener = update -> {
            latest.set(update);
            synchronized (responseObserver) {
                try {
                    responseObserver.onNext(update);
                } catch (Exception e) {
                    System.err.println("Worker " + workerId + " failed to stream progress for task "
                            + request.getTaskId() + ": " + e.getMessage());
                }
            }
        };

        // Submit task to background thread pool to keep gRPC network thread non-blocking
        Future<TaskResultResponse> future = executionEngine.submitTask(request, progressListener);

        // When task completes, emit a final completion progress event and close the stream
        new Thread(() -> {
            try {
                TaskResultResponse result = future.get();
                synchronized (responseObserver) {
                    // Send final 100% or completion status
                    TaskProgressUpdate previous = latest.get();
                    TaskProgressUpdate finalUpdate = TaskProgressUpdate.newBuilder()
                            .setJobId(request.getJobId())
                            .setTaskId(request.getTaskId())
                            .setWorkerId(workerId)
                            .setProgressPercentage(result.getSuccess() ? 100.0 : previous == null ? 0 : previous.getProgressPercentage())
                            .setUnitsCompleted(previous == null ? 0 : previous.getUnitsCompleted())
                            .setTotalUnits(previous == null ? 0 : previous.getTotalUnits())
                            .setAttempt(request.getAttempt()).setCoordinatorTerm(request.getCoordinatorTerm())
                            .setFinalResult(result.toBuilder().setAttempt(request.getAttempt()).setCoordinatorTerm(request.getCoordinatorTerm()))
                            .setStatusMessage(result.getSuccess() ? "COMPLETED" : "FAILED: " + result.getErrorMessage())
                            .setTimestampMs(System.currentTimeMillis())
                            .build();
                    responseObserver.onNext(finalUpdate);
                    responseObserver.onCompleted();
                }
            } catch (Exception e) {
                synchronized (responseObserver) {
                    responseObserver.onError(e);
                }
            }
        }, "worker-" + workerId + "-stream-monitor-" + request.getTaskId()).start();
    }

    /**
     * Synchronous / Unary task dispatch option.
     */
    @Override
    public void executeTask(TaskDispatchRequest request, StreamObserver<TaskResultResponse> responseObserver) {
        Future<TaskResultResponse> future = executionEngine.submitTask(request, null);
        new Thread(() -> {
            try {
                TaskResultResponse result = future.get();
                responseObserver.onNext(result);
                responseObserver.onCompleted();
            } catch (Exception e) {
                responseObserver.onError(e);
            }
        }, "worker-" + workerId + "-unary-monitor-" + request.getTaskId()).start();
    }

    /**
     * Coordinator ping/heartbeat endpoint.
     * Reports current worker status and active load so Coordinator can classify:
     * - Active (responsive, idle/healthy load)
     * - Slow (responsive, high active tasks count or delayed response)
     * - Dead (gRPC timeout / UNAVAILABLE error)
     */
    @Override
    public void ping(HeartbeatRequest request, StreamObserver<HeartbeatResponse> responseObserver) {
        int activeTasks = executionEngine.getActiveTaskCount();
        HeartbeatResponse.WorkerStatus status = (activeTasks > 0)
                ? HeartbeatResponse.WorkerStatus.BUSY
                : HeartbeatResponse.WorkerStatus.IDLE;

        HeartbeatResponse response = HeartbeatResponse.newBuilder()
                .setWorkerId(workerId)
                .setStatus(status)
                .setActiveTasksCount(activeTasks)
                .setCurrentTerm(currentTerm)
                .setTimestampMs(System.currentTimeMillis())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Coordinator preemption/cancellation endpoint.
     */
    @Override
    public void cancelTask(CancelTaskRequest request, StreamObserver<CancelTaskResponse> responseObserver) {
        CancelTaskResponse response = executionEngine.cancelTask(request);
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
