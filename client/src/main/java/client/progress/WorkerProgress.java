package client.progress;

/**
 * One worker's share of a job, as reported by the coordinator
 * (WorkerJobProgress in coordinator.proto).
 *
 * Progress is measured in work units (numbers checked, or range values
 * evaluated) as reported by the workers, never by a timer.
 */
public record WorkerProgress(int workerId, long unitsCompleted, long totalUnits) {

    /** This worker's own completion, 0-100. */
    public double percent() {
        if (totalUnits <= 0) {
            return 0.0;
        }
        return Math.min(100.0, (unitsCompleted * 100.0) / totalUnits);
    }

    /** True once the worker has processed all of its units. */
    public boolean isDone() {
        return totalUnits > 0 && unitsCompleted >= totalUnits;
    }
}
