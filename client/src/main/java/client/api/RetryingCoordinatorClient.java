package client.api;

import client.model.JobSpec;
import client.progress.ProgressSnapshot;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Wraps any CoordinatorClient and resubmits a job after a retryable failure,
 * e.g. the coordinator crashed or is being replaced by a new election.
 *
 * It only uses the CoordinatorClient interface, so it is tested without a
 * network. Every retry reuses the SAME JobSpec, so the job ID never changes.
 * coordinator.proto makes SubmitJob with a known ID an "exact retry" that
 * starts no new work, so retrying cannot run a job twice; the client then
 * re-attaches to the job's progress stream (WatchJob).
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
            public void onProgress(ProgressSnapshot snapshot) {
                if (!finished.get()) {
                    listener.onProgress(snapshot);
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
