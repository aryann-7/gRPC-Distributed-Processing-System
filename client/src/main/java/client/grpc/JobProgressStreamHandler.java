package client.grpc;

import client.api.JobProgressListener;
import common.grpc.JobProgress;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Receives the coordinator's WatchJob stream for ONE job and turns it into
 * listener calls. This is the client-side streaming listener.
 *
 * gRPC calls these methods on its own threads, one message at a time:
 *   onNext      - one JobProgress snapshot (QUEUED, RUNNING, COMPLETED or FAILED)
 *   onError     - the stream broke (e.g. the coordinator crashed)
 *   onCompleted - the coordinator closed the stream normally
 *
 * Exactly one final outcome is reported, guarded by an AtomicBoolean,
 * whichever of the above happens first.
 */
public class JobProgressStreamHandler implements StreamObserver<JobProgress> {

    private final JobProgressListener listener;
    private final Runnable onConnectionLost;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    /**
     * @param onConnectionLost called when the stream fails at transport level,
     *                         so the caller can drop the dead channel
     */
    public JobProgressStreamHandler(JobProgressListener listener, Runnable onConnectionLost) {
        this.listener = listener;
        this.onConnectionLost = onConnectionLost;
    }

    @Override
    public void onNext(JobProgress message) {
        if (finished.get()) {
            return;
        }
        // Every message is a full snapshot, including the final one.
        listener.onProgress(ProtoMapper.toSnapshot(message));

        switch (message.getState()) {
            case COMPLETED:
                if (finished.compareAndSet(false, true)) {
                    if (message.hasResultValue()) {
                        listener.onCompleted(message.getResultValue());
                    } else {
                        listener.onFailed("Coordinator reported COMPLETED without a result", false);
                    }
                }
                break;
            case FAILED:
                // A FAILED job is final: the coordinator itself decided it
                // cannot be done (e.g. bad input), so the client does not retry.
                if (finished.compareAndSet(false, true)) {
                    String error = message.hasErrorMessage() && !message.getErrorMessage().isEmpty()
                            ? message.getErrorMessage() : "Coordinator reported failure";
                    listener.onFailed(error, false);
                }
                break;
            default:
                // QUEUED / RUNNING: progress only, already forwarded.
                break;
        }
    }

    @Override
    public void onError(Throwable t) {
        if (!finished.compareAndSet(false, true)) {
            return;
        }
        Status status = Status.fromThrowable(t);
        boolean retryable = isRetryable(status.getCode());
        if (retryable) {
            onConnectionLost.run();
        }
        listener.onFailed("Connection to coordinator lost (" + describe(status) + ")", retryable);
    }

    @Override
    public void onCompleted() {
        // The coordinator always sends COMPLETED or FAILED before closing.
        // A stream closed early does not cancel the job (coordinator.proto),
        // so reconnecting with the same job ID is the right response.
        if (finished.compareAndSet(false, true)) {
            listener.onFailed("Coordinator closed the stream before the job finished", true);
        }
    }

    /**
     * Problems where trying again (after the cluster recovers or elects a new
     * coordinator) is expected to work. Other codes mean a bug or bad input.
     */
    public static boolean isRetryable(Status.Code code) {
        switch (code) {
            case UNAVAILABLE:         // coordinator/bootstrap process gone or not listening
            case DEADLINE_EXCEEDED:   // did not answer in time
            case ABORTED:             // coordinator gave up the job (e.g. stepping down)
            case CANCELLED:           // stream cancelled by the server
            case FAILED_PRECONDITION: // coordinator.proto: "An inactive coordinator returns FAILED_PRECONDITION"
                return true;
            default:
                return false;
        }
    }

    /** "CODE" or "CODE - description", for log messages. */
    static String describe(Status status) {
        return status.getDescription() == null
                ? status.getCode().toString()
                : status.getCode() + " - " + status.getDescription();
    }
}
