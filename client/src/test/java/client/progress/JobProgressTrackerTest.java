package client.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;

public class JobProgressTrackerTest {

    private static final double EPS = 1e-9;

    private static ProgressSnapshot running(long done, long total, WorkerProgress... workers) {
        return new ProgressSnapshot(false, done, total, List.of(workers));
    }

    @Test
    public void startsAtZero() {
        assertEquals(0.0, new JobProgressTracker("j", "MAX").overallPercent(), EPS);
    }

    @Test
    public void overallUsesJobTotalsNotAnAverageOfWorkers() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        // Worker 101 finished its 900 units, worker 102 has done 0 of 100.
        // 900 of 1000 units = 90%; a plain average of 100% and 0% would say 50%.
        job.apply(running(900, 1000, new WorkerProgress(101, 900, 900), new WorkerProgress(102, 0, 100)));
        assertEquals(90.0, job.overallPercent(), EPS);
    }

    @Test
    public void unallocatedWorkKeepsTheBarBelowHundred() {
        JobProgressTracker job = new JobProgressTracker("j", "PRIMESUM");
        // The only allocated worker is done, but the job has 1000 units in total.
        job.apply(running(250, 1000, new WorkerProgress(101, 250, 250)));
        assertEquals(25.0, job.overallPercent(), EPS);
    }

    @Test
    public void newerSnapshotReplacesOlder() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.apply(running(10, 100, new WorkerProgress(101, 10, 100)));
        job.apply(running(60, 100, new WorkerProgress(101, 60, 100)));
        assertEquals(60.0, job.overallPercent(), EPS);
        assertEquals(1, job.workerBreakdown().size());
        assertEquals(60, job.unitsCompleted());
    }

    @Test
    public void queuedThenRunningStates() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.apply(new ProgressSnapshot(true, 0, 100, List.of()));
        assertEquals(JobProgressTracker.State.QUEUED, job.getState());
        job.apply(running(5, 100, new WorkerProgress(101, 5, 50)));
        assertEquals(JobProgressTracker.State.RUNNING, job.getState());
    }

    @Test
    public void breakdownIsSortedByWorker() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.apply(running(2, 20, new WorkerProgress(103, 1, 10), new WorkerProgress(101, 1, 10)));
        assertEquals(101, job.workerBreakdown().get(0).workerId());
    }

    @Test
    public void completeGivesHundredPercentAndResult() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.complete(42);
        assertEquals(100.0, job.overallPercent(), EPS);
        assertEquals(42L, (long) job.getResult());
        assertTrue(job.isFinished());
    }

    @Test
    public void lateSnapshotDoesNotUndoCompletion() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.complete(42);
        job.apply(running(5, 10));
        assertEquals(JobProgressTracker.State.COMPLETED, job.getState());
    }

    @Test
    public void restartClearsOldAttemptProgress() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.apply(running(50, 100, new WorkerProgress(101, 50, 100)));
        job.restart(2);
        assertEquals(0.0, job.overallPercent(), EPS);
        assertTrue(job.workerBreakdown().isEmpty());
        assertEquals(JobProgressTracker.State.RETRYING, job.getState());
        assertEquals(2, job.getAttempt());
    }
}
