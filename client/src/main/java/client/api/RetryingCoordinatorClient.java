package client.api;

import client.model.JobSpec;
import client.progress.WorkerProgress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps any CoordinatorClient and resubmits a job after a retryable failure,
 * e.g. the coordinator crashed or is being replaced by a new election.
 *
 * Works the same for RMI and gRPC because it only uses the interface.
 * The resubmission keeps the same job ID, so a coordinator that remembers
 * job IDs can recognise it as the same job.
 */
public class RetryingCoordinatorClient implements CoordinatorClient {

    private final CoordinatorClient delegate;
    private final int maxAttempts;
    private final long delayMs;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "client-retry-scheduler");
                t.setDaemon(true);
                return t;
            });

    public RetryingCoordinatorClient(CoordinatorClient delegate, int maxAttempts, long delayMs) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be at least 1");
        }
        this.delegate = delegate;
        this.maxAttempts = maxAttempts;
        this.delayMs = delayMs;
    }

    @Override
    public void submit(JobSpec job, JobProgressListener listener) {
        attempt(job, listener, 1);
    }

    private void attempt(JobSpec job, JobProgressListener listener, int attemptNo) {
        // Guards against a buggy transport reporting both success and failure.
        AtomicBoolean finished = new AtomicBoolean(false);

        delegate.submit(job, new JobProgressListener() {
            @Override
            public void onProgress(WorkerProgress progress) {
                if (!finished.get()) {
                    listener.onProgress(progress);
                }
            }

            @Override
            public void onCompleted(long result) {
                if (finished.compareAndSet(false, true)) {
                    listener.onCompleted(result);
                }
            }

            @Override
            public void onFailed(String message, boolean retryable) {
                if (!finished.compareAndSet(false, true)) {
                    return;
                }
                if (retryable && attemptNo < maxAttempts) {
                    int next = attemptNo + 1;
                    listener.onRetry(next, maxAttempts, message);
                    scheduler.schedule(() -> attempt(job, listener, next),
                            delayMs, TimeUnit.MILLISECONDS);
                } else {
                    String suffix = retryable ? " (gave up after " + maxAttempts + " attempts)" : "";
                    listener.onFailed(message + suffix, retryable);
                }
            }
        });
    }

    @Override
    public String checkConnection() throws Exception {
        return delegate.checkConnection();
    }

    @Override
    public void close() {
        scheduler.shutdownNow();
        delegate.close();
    }
}
