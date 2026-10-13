package client.progress;

/**
 * Snapshot of one worker's sub-task for one job.
 *
 * Progress is measured in work units (numbers checked, or range values
 * evaluated) as reported by the worker, never by a timer.
 */
public record WorkerProgress(int workerId, String taskId,
                             long unitsCompleted, long totalUnits, String status) {

    /** This worker's own completion, 0-100. */
    public double percent() {
        if (totalUnits <= 0) {
            return 0.0;
        }
        return Math.min(100.0, (unitsCompleted * 100.0) / totalUnits);
    }
}
