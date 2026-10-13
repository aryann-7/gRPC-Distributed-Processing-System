package client.grpc;

import client.api.JobProgressListener;
import common.grpc.client.JobProgress;
import common.grpc.client.WorkerTaskProgress;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Receives the coordinator's progress stream for ONE job and turns it into
 * listener calls. This is the client-side streaming listener.
 *
 * gRPC calls these methods on its own threads, one message at a time:
 *   onNext      - one JobProgress message (RUNNING, COMPLETED or FAILED)
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
        for (WorkerTaskProgress worker : message.getWorkersList()) {
            listener.onProgress(ProtoMapper.fromProto(worker));
        }
        switch (message.getState()) {
            case COMPLETED:
                if (finished.compareAndSet(false, true)) {
                    listener.onCompleted(message.getResultValue());
                }
                break;
            case FAILED:
                if (finished.compareAndSet(false, true)) {
                    String error = message.getErrorMessage().isEmpty()
                            ? "Coordinator reported failure" : message.getErrorMessage();
                    listener.onFailed(error, message.getRetryable());
                }
                break;
            default:
                // RUNNING (or unspecified): progress only, already forwarded.
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
        String detail = status.getDescription() == null ? "" : " - " + status.getDescription();
        listener.onFailed("Connection to coordinator lost (" + status.getCode() + detail + ")", retryable);
    }

    @Override
    public void onCompleted() {
        // A correct coordinator always sends COMPLETED or FAILED before closing.
        if (finished.compareAndSet(false, true)) {
            listener.onFailed("Coordinator closed the stream before the job finished", true);
        }
    }

    /**
     * Transport problems where trying again (after a new coordinator is
     * elected) is expected to work. Other codes mean a bug or bad input.
     */
    public static boolean isRetryable(Status.Code code) {
        switch (code) {
            case UNAVAILABLE:        // coordinator process gone / not listening
            case DEADLINE_EXCEEDED:  // did not answer in time
            case ABORTED:            // coordinator gave up the job (e.g. stepping down)
            case CANCELLED:          // stream cancelled by the server
                return true;
            default:
                return false;
        }
    }
}
