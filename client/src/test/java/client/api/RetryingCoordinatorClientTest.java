package client.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import client.model.JobKind;
import client.model.JobSpec;
import client.progress.WorkerProgress;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

public class RetryingCoordinatorClientTest {

    private static final JobSpec JOB = JobSpec.forNumbers("job-test", JobKind.MAX, List.of(1L, 2L));

    /** Fake transport that plays back a scripted outcome per attempt. */
    private static class ScriptedClient implements CoordinatorClient {
        private final Deque<String> script;
        final List<String> submittedJobIds = new CopyOnWriteArrayList<>();

        ScriptedClient(String... outcomes) {
            script = new ArrayDeque<>(List.of(outcomes));
        }

        @Override
        public void submit(JobSpec job, JobProgressListener listener) {
            submittedJobIds.add(job.jobId());
            String outcome = script.poll();
            new Thread(() -> {
                switch (outcome) {
                    case "ok" -> {
                        listener.onProgress(new WorkerProgress(101, "t1", 2, 2, "COMPLETED"));
                        listener.onCompleted(99);
                    }
                    case "retryable" -> listener.onFailed("coordinator gone", true);
                    default -> listener.onFailed("bad input", false);
                }
            }).start();
        }

        @Override public String checkConnection() { return "fake"; }
        @Override public void close() { }
    }

    /** Records what the wrapper reports back. */
    private static class Recorder implements JobProgressListener {
        final CountDownLatch done = new CountDownLatch(1);
        volatile Long result;
        volatile String failure;
        volatile boolean failureRetryable;
        final List<Integer> retries = new CopyOnWriteArrayList<>();

        @Override public void onProgress(WorkerProgress p) { }
        @Override public void onCompleted(long r) { result = r; done.countDown(); }
        @Override public void onFailed(String m, boolean retryable) {
            failure = m;
            failureRetryable = retryable;
            done.countDown();
        }
        @Override public void onRetry(int attempt, int max, String reason) { retries.add(attempt); }
    }

    @Test
    public void succeedsAfterRetryableFailuresAndKeepsSameJobId() throws Exception {
        ScriptedClient fake = new ScriptedClient("retryable", "retryable", "ok");
        Recorder rec = new Recorder();
        try (RetryingCoordinatorClient client = new RetryingCoordinatorClient(fake, 3, 10)) {
            client.submit(JOB, rec);
            assertTrue(rec.done.await(5, TimeUnit.SECONDS));
        }
        assertEquals(99L, (long) rec.result);
        assertEquals(List.of(2, 3), rec.retries);
        assertEquals(List.of("job-test", "job-test", "job-test"), fake.submittedJobIds);
    }

    @Test
    public void givesUpAfterMaxAttempts() throws Exception {
        ScriptedClient fake = new ScriptedClient("retryable", "retryable");
        Recorder rec = new Recorder();
        try (RetryingCoordinatorClient client = new RetryingCoordinatorClient(fake, 2, 10)) {
            client.submit(JOB, rec);
            assertTrue(rec.done.await(5, TimeUnit.SECONDS));
        }
        assertTrue(rec.failure.contains("gave up after 2 attempts"));
        assertEquals(2, fake.submittedJobIds.size());
    }

    @Test
    public void doesNotRetryNonRetryableFailure() throws Exception {
        ScriptedClient fake = new ScriptedClient("fatal");
        Recorder rec = new Recorder();
        try (RetryingCoordinatorClient client = new RetryingCoordinatorClient(fake, 3, 10)) {
            client.submit(JOB, rec);
            assertTrue(rec.done.await(5, TimeUnit.SECONDS));
        }
        assertEquals("bad input", rec.failure);
        assertFalse(rec.failureRetryable);
        assertEquals(1, fake.submittedJobIds.size());
        assertTrue(rec.retries.isEmpty());
    }
}
