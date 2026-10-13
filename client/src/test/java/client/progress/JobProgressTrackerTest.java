package client.progress;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

public class JobProgressTrackerTest {

    private static final double EPS = 1e-9;

    @Test
    public void startsAtZero() {
        assertEquals(0.0, new JobProgressTracker("j", "MAX").overallPercent(), EPS);
    }

    @Test
    public void overallIsWeightedByUnitsNotAveraged() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.update(new WorkerProgress(101, "t1", 900, 900, "COMPLETED"));
        job.update(new WorkerProgress(102, "t2", 0, 100, "PROCESSING"));
        // 900 of 1000 units done; a plain average of 100% and 0% would say 50%.
        assertEquals(90.0, job.overallPercent(), EPS);
    }

    @Test
    public void newerReportReplacesOlderForSameTask() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.update(new WorkerProgress(101, "t1", 10, 100, "PROCESSING"));
        job.update(new WorkerProgress(101, "t1", 60, 100, "PROCESSING"));
        assertEquals(60.0, job.overallPercent(), EPS);
        assertEquals(1, job.workerBreakdown().size());
    }

    @Test
    public void breakdownIsSortedByWorker() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.update(new WorkerProgress(103, "t3", 1, 10, "PROCESSING"));
        job.update(new WorkerProgress(101, "t1", 1, 10, "PROCESSING"));
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
    public void restartClearsOldAttemptProgress() {
        JobProgressTracker job = new JobProgressTracker("j", "MAX");
        job.update(new WorkerProgress(101, "t1", 50, 100, "PROCESSING"));
        job.restart(2);
        assertEquals(0.0, job.overallPercent(), EPS);
        assertEquals(JobProgressTracker.State.RETRYING, job.getState());
        assertEquals(2, job.getAttempt());
    }
}
