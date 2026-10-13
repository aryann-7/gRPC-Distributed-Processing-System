package client.progress;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds the live progress of one submitted job.
 *
 * Updates arrive on gRPC threads while the GUI reads on the Swing thread,
 * so the per-task map is a ConcurrentHashMap and the job state is volatile.
 */
public class JobProgressTracker {

    public enum State { SUBMITTED, RUNNING, RETRYING, COMPLETED, FAILED }

    private final String jobId;
    private final String jobType;
    private final Map<String, WorkerProgress> tasks = new ConcurrentHashMap<>();

    private volatile State state = State.SUBMITTED;
    private volatile Long result;
    private volatile String errorMessage;
    private volatile int attempt = 1;

    public JobProgressTracker(String jobId, String jobType) {
        this.jobId = jobId;
        this.jobType = jobType;
    }

    /** Records the latest progress for one sub-task (newest report wins). */
    public void update(WorkerProgress progress) {
        tasks.put(progress.taskId(), progress);
        if (state == State.SUBMITTED || state == State.RETRYING) {
            state = State.RUNNING;
        }
    }

    public void complete(long resultValue) {
        this.result = resultValue;
        this.state = State.COMPLETED;
    }

    public void fail(String message) {
        this.errorMessage = message;
        this.state = State.FAILED;
    }

    /**
     * The job is being submitted again (e.g. after a coordinator change).
     * Progress from the abandoned attempt is cleared so the bar does not
     * mix old and new work.
     */
    public void restart(int attemptNo) {
        tasks.clear();
        this.attempt = attemptNo;
        this.state = State.RETRYING;
    }

    /**
     * Overall completion = total units completed / total units, across all
     * sub-tasks. Not an average of percentages: chunks can differ in size,
     * and a plain average would over- or under-state progress.
     */
    public double overallPercent() {
        if (state == State.COMPLETED) {
            return 100.0;
        }
        long done = 0;
        long total = 0;
        for (WorkerProgress p : tasks.values()) {
            done += p.unitsCompleted();
            total += p.totalUnits();
        }
        return total == 0 ? 0.0 : Math.min(100.0, (done * 100.0) / total);
    }

    /** Copy of the current per-worker rows, sorted by worker ID for display. */
    public List<WorkerProgress> workerBreakdown() {
        List<WorkerProgress> rows = new ArrayList<>(tasks.values());
        rows.sort(Comparator.comparingInt(WorkerProgress::workerId)
                .thenComparing(WorkerProgress::taskId));
        return rows;
    }

    public String getJobId() { return jobId; }
    public String getJobType() { return jobType; }
    public State getState() { return state; }
    public Long getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
    public int getAttempt() { return attempt; }

    /** True once the job has a final outcome. */
    public boolean isFinished() {
        return state == State.COMPLETED || state == State.FAILED;
    }
}
