package client.progress;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Holds the live progress of one submitted job.
 *
 * Updates arrive on gRPC threads while the GUI reads on the Swing thread.
 * Each update is one immutable ProgressSnapshot stored in a volatile field,
 * so a reader always sees one whole snapshot, never half of an update.
 */
public class JobProgressTracker {

    public enum State { SUBMITTED, QUEUED, RUNNING, RETRYING, COMPLETED, FAILED }

    private final String jobId;
    private final String jobType;

    private volatile ProgressSnapshot latest;
    private volatile State state = State.SUBMITTED;
    private volatile Long result;
    private volatile String errorMessage;
    private volatile int attempt = 1;
    private volatile double lastCapturedPercent = 0.0;

    public JobProgressTracker(String jobId, String jobType) {
        this.jobId = jobId;
        this.jobType = jobType;
    }

    /** Stores the coordinator's newest snapshot (it replaces the old one). */
    public void apply(ProgressSnapshot snapshot) {
        if (isFinished()) {
            return;
        }
        this.latest = snapshot;
        if (snapshot.totalUnits() > 0) {
            this.lastCapturedPercent = Math.min(100.0, (snapshot.unitsCompleted() * 100.0) / snapshot.totalUnits());
        }
        state = snapshot.queued() ? State.QUEUED : State.RUNNING;
    }

    public void complete(long resultValue) {
        this.result = resultValue;
        this.lastCapturedPercent = 100.0;
        this.state = State.COMPLETED;
    }

    public void fail(String message) {
        this.errorMessage = message;
        this.state = State.FAILED;
    }

    /**
     * The client is reconnecting (e.g. after a coordinator change). The old
     * snapshot is cleared; the new coordinator's first message is a fresh
     * full snapshot that refills it.
     */
    public void restart(int attemptNo) {
        this.latest = null;
        this.attempt = attemptNo;
        this.state = State.RETRYING;
        this.lastCapturedPercent = 0.0;
    }

    /**
     * Overall completion = job units completed / job total units, as counted
     * by the coordinator. The total includes work not yet handed to any
     * worker, so the bar never jumps to 100% early. Not an average of the
     * workers' percentages, which would ignore that chunks differ in size.
     * When cancelled or failed, progress stops and freezes at the exact value achieved.
     */
    public double overallPercent() {
        if (state == State.COMPLETED) {
            return 100.0;
        }
        if (state == State.FAILED) {
            return lastCapturedPercent;
        }
        ProgressSnapshot s = latest;
        if (s == null || s.totalUnits() <= 0) {
            return lastCapturedPercent;
        }
        double pct = Math.min(100.0, (s.unitsCompleted() * 100.0) / s.totalUnits());
        lastCapturedPercent = pct;
        return pct;
    }

    /** Job-wide units done, 0 before the first snapshot. */
    public long unitsCompleted() {
        ProgressSnapshot s = latest;
        return s == null ? 0 : s.unitsCompleted();
    }

    /** Job-wide total units, 0 before the first snapshot. */
    public long totalUnits() {
        ProgressSnapshot s = latest;
        return s == null ? 0 : s.totalUnits();
    }

    /** Copy of the current per-worker rows, sorted by worker ID for display. */
    public List<WorkerProgress> workerBreakdown() {
        ProgressSnapshot s = latest;
        if (s == null) {
            return List.of();
        }
        List<WorkerProgress> rows = new ArrayList<>(s.workers());
        rows.sort(Comparator.comparingInt(WorkerProgress::workerId));
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
