package worker.computation;

import common.grpc.CancelTaskRequest;
import common.grpc.CancelTaskResponse;
import common.grpc.TaskDispatchRequest;
import common.grpc.TaskResultResponse;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe execution manager for the Worker Node.
 * Manages thread pools, active task tracking, and task cancellation.
 */
public class TaskExecutionEngine {

    private final int workerId;
    private final ExecutorService threadPool;
    private final Map<String, GranularJobRunner> activeRunners = new ConcurrentHashMap<>();
    private final Map<String, Future<TaskResultResponse>> activeFutures = new ConcurrentHashMap<>();
    private final AtomicInteger activeTaskCount = new AtomicInteger(0);

    public TaskExecutionEngine(int workerId, int threadPoolSize) {
        this.workerId = workerId;
        this.threadPool = Executors.newFixedThreadPool(threadPoolSize, new ThreadFactory() {
            private final AtomicInteger count = new AtomicInteger(1);
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "worker-" + workerId + "-thread-" + count.getAndIncrement());
                t.setDaemon(true);
                return t;
            }
        });
    }

    public int getActiveTaskCount() {
        return activeTaskCount.get();
    }

    /**
     * Submits a computational sub-task to the thread pool with streaming progress updates.
     */
    public Future<TaskResultResponse> submitTask(TaskDispatchRequest request, ProgressListener progressListener) {
        String taskId = request.getTaskId();
        GranularJobRunner runner = new GranularJobRunner(request, workerId, progressListener);
        activeRunners.put(taskId, runner);
        activeTaskCount.incrementAndGet();

        Future<TaskResultResponse> future = threadPool.submit(() -> {
            try {
                return runner.call();
            } finally {
                activeRunners.remove(taskId);
                activeFutures.remove(taskId);
                activeTaskCount.decrementAndGet();
            }
        });

        activeFutures.put(taskId, future);
        return future;
    }

    /**
     * Cancels an ongoing task if requested by Coordinator.
     */
    public CancelTaskResponse cancelTask(CancelTaskRequest request) {
        String taskId = request.getTaskId();
        GranularJobRunner runner = activeRunners.get(taskId);
        Future<TaskResultResponse> future = activeFutures.get(taskId);

        if (runner == null && future == null) {
            return CancelTaskResponse.newBuilder()
                    .setTaskId(taskId)
                    .setCancelled(false)
                    .setMessage("Task " + taskId + " not found or already completed")
                    .build();
        }

        if (runner != null) {
            runner.cancel();
        }
        if (future != null) {
            future.cancel(true);
        }

        activeRunners.remove(taskId);
        activeFutures.remove(taskId);

        return CancelTaskResponse.newBuilder()
                .setTaskId(taskId)
                .setCancelled(true)
                .setMessage("Task " + taskId + " successfully cancelled")
                .build();
    }

    public void shutdown() {
        threadPool.shutdownNow();
    }
}
