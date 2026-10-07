package worker.computation;

import common.grpc.TaskProgressUpdate;

@FunctionalInterface
public interface ProgressListener {
    /**
     * Invoked when computational units have been processed.
     * Guaranteed to reflect actual physical computation completed.
     *
     * @param update Progress event containing percentage and completed unit count.
     */
    void onProgress(TaskProgressUpdate update);
}
