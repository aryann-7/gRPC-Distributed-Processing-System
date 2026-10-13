package client.api;

import client.progress.ProgressSnapshot;

/**
 * Callbacks for one submitted job.
 *
 * Called from background (network) threads, never the Swing thread, so
 * implementations must not touch Swing components directly.
 * Exactly one of onCompleted / onFailed is called, once, per job.
 */
public interface JobProgressListener {

    /** The coordinator sent a new full progress snapshot for the job. */
    void onProgress(ProgressSnapshot snapshot);

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
