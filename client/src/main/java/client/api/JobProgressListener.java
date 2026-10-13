package client.api;

import client.progress.WorkerProgress;

/**
 * Callbacks for one submitted job.
 *
 * Called from background (network) threads, never the Swing thread, so
 * implementations must not touch Swing components directly.
 * Exactly one of onCompleted / onFailed is called, once, per job.
 */
public interface JobProgressListener {

    /** A worker reported how much of its sub-task is done. */
    void onProgress(WorkerProgress progress);

    /** The job finished; result is the aggregated answer. */
    void onCompleted(long result);

    /**
     * The job failed.
     *
     * @param retryable true for transient problems (coordinator unreachable,
     *                  coordinator changing after an election), where
     *                  submitting again is expected to work
     */
    void onFailed(String message, boolean retryable);

    /** The job is about to be submitted again after a retryable failure. */
    default void onRetry(int attempt, int maxAttempts, String reason) {
    }
}
