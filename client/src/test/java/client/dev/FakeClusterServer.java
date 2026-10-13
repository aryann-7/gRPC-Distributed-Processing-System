package client.dev;

import common.grpc.client.ClusterDiscoveryGrpc;
import common.grpc.client.CoordinatorEndpoint;
import common.grpc.client.CoordinatorJobServiceGrpc;
import common.grpc.client.CoordinatorQuery;
import common.grpc.client.CoordinatorStatus;
import common.grpc.client.JobProgress;
import common.grpc.client.JobSubmission;
import common.grpc.client.StatusRequest;
import common.grpc.client.WorkerTaskProgress;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * DEVELOPMENT TOOL ONLY (test sources, never shipped, never used in the demo).
 *
 * A stand-in for the real cluster so the real client can be run and clicked
 * through before teammates finish the Bootstrap and Coordinator:
 *   - fake Bootstrap  : ClusterDiscovery on port 50050
 *   - fake Coordinator: CoordinatorJobService on port 50060
 *
 * Its "workers" are 3 threads in this process that genuinely compute
 * MAX / PRIMESUM / PRIMECOUNT on their share of the job and report units
 * actually completed. Results are correct; there are no timers or sleeps.
 *
 *   mvn -q test-compile exec:java -Dexec.classpathScope=test
 *       -Dexec.mainClass=client.dev.FakeClusterServer [-Dexec.args="--crash-every 3"]
 *
 * --crash-every N : every Nth job is cut off part-way with UNAVAILABLE, like a
 *                   crashed coordinator, so the client's automatic retry shows.
 */
public class FakeClusterServer {

    private static final int BOOTSTRAP_PORT = 50050;
    private static final int COORDINATOR_PORT = 50060;
    private static final int COORDINATOR_ID = 103;
    private static final int[] WORKER_IDS = {101, 102, 103};
    private static final int REPORT_EVERY_UNITS = 5_000;

    public static void main(String[] args) throws Exception {
        int crashEvery = 0;
        for (int i = 0; i + 1 < args.length; i++) {
            if ("--crash-every".equals(args[i])) {
                crashEvery = Integer.parseInt(args[i + 1]);
            }
        }

        Server bootstrap = ServerBuilder.forPort(BOOTSTRAP_PORT).addService(new Discovery()).build().start();
        Server coordinator = ServerBuilder.forPort(COORDINATOR_PORT)
                .addService(new Coordinator(crashEvery)).build().start();
        System.out.println("FAKE cluster for client development (not the real system)");
        System.out.println("  Bootstrap discovery on port " + BOOTSTRAP_PORT);
        System.out.println("  Coordinator (Worker " + COORDINATOR_ID + ") on port " + COORDINATOR_PORT);
        if (crashEvery > 0) {
            System.out.println("  Every job number divisible by " + crashEvery + " will be cut off to test client retry");
        }
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            bootstrap.shutdownNow();
            coordinator.shutdownNow();
        }));
        bootstrap.awaitTermination();
    }

    private static class Discovery extends ClusterDiscoveryGrpc.ClusterDiscoveryImplBase {
        @Override
        public void getCoordinator(CoordinatorQuery request, StreamObserver<CoordinatorEndpoint> out) {
            out.onNext(CoordinatorEndpoint.newBuilder().setAvailable(true).setHost("localhost")
                    .setPort(COORDINATOR_PORT).setCoordinatorId(COORDINATOR_ID).setTerm(1).build());
            out.onCompleted();
        }
    }

    private static class Coordinator extends CoordinatorJobServiceGrpc.CoordinatorJobServiceImplBase {
        private final int crashEvery;
        private final AtomicInteger jobsReceived = new AtomicInteger();
        private final ExecutorService workers = Executors.newCachedThreadPool();

        Coordinator(int crashEvery) {
            this.crashEvery = crashEvery;
        }

        @Override
        public void getStatus(StatusRequest request, StreamObserver<CoordinatorStatus> out) {
            out.onNext(CoordinatorStatus.newBuilder().setCoordinatorId(COORDINATOR_ID).setTerm(1)
                    .setActiveWorkers(WORKER_IDS.length).setJobsInTerm(jobsReceived.get()).build());
            out.onCompleted();
        }

        @Override
        public void submitJob(JobSubmission job, StreamObserver<JobProgress> out) {
            int jobNo = jobsReceived.incrementAndGet();
            boolean crash = crashEvery > 0 && jobNo % crashEvery == 0;
            System.out.println("Job " + job.getJobId() + " (" + job.getJobType() + ") from client "
                    + job.getClientId() + (crash ? "  [will be cut off]" : ""));
            workers.execute(() -> run(job, out, crash));
        }

        private void run(JobSubmission job, StreamObserver<JobProgress> out, boolean crash) {
            int n = WORKER_IDS.length;
            long[] start = new long[n];
            long[] total = new long[n];
            split(job, start, total);
            AtomicLongArray done = new AtomicLongArray(n);
            Object streamLock = new Object();
            boolean[] closed = {false};   // guarded by streamLock

            List<Future<Long>> parts = new ArrayList<>();
            for (int w = 0; w < n; w++) {
                final int idx = w;
                parts.add(workers.submit(() -> compute(job, idx, start[idx], total[idx], units -> {
                    done.set(idx, units);
                    synchronized (streamLock) {   // gRPC streams are not thread-safe
                        if (!closed[0]) {
                            out.onNext(snapshot(job, JobProgress.JobState.RUNNING, start.length, done, total));
                        }
                    }
                })));
            }
            try {
                if (crash) {
                    Thread.sleep(50);
                    synchronized (streamLock) {
                        closed[0] = true;
                        out.onError(Status.UNAVAILABLE.withDescription("fake coordinator crash").asRuntimeException());
                    }
                    return;
                }
                long result = combine(job, parts);
                synchronized (streamLock) {
                    closed[0] = true;
                    out.onNext(snapshot(job, JobProgress.JobState.COMPLETED, n, done, total).toBuilder()
                            .setResultValue(result).build());
                    out.onCompleted();
                }
                System.out.println("  " + job.getJobId() + " = " + result);
            } catch (Exception e) {
                synchronized (streamLock) {
                    closed[0] = true;
                    out.onNext(JobProgress.newBuilder().setJobId(job.getJobId())
                            .setState(JobProgress.JobState.FAILED).setErrorMessage(e.toString()).build());
                    out.onCompleted();
                }
            }
        }

        /** Even split: list indexes for MAX/PRIMECOUNT, number range for PRIMESUM. */
        private static void split(JobSubmission job, long[] start, long[] total) {
            boolean range = job.getJobType() == common.grpc.client.ClientJobType.PRIMESUM;
            long from = range ? Math.min(job.getRangeStart(), job.getRangeEnd()) : 0;
            long size = range ? Math.abs(job.getRangeEnd() - job.getRangeStart()) + 1 : job.getNumbersCount();
            long base = size / start.length;
            long extra = size % start.length;
            long next = from;
            for (int w = 0; w < start.length; w++) {
                start[w] = next;
                total[w] = base + (w < extra ? 1 : 0);
                next += total[w];
            }
        }

        private interface Reporter {
            void report(long unitsDone);
        }

        private static long compute(JobSubmission job, int w, long first, long count, Reporter reporter) {
            long acc = job.getJobType() == common.grpc.client.ClientJobType.MAX ? Long.MIN_VALUE : 0;
            for (long i = 0; i < count; i++) {
                switch (job.getJobType()) {
                    case MAX -> acc = Math.max(acc, job.getNumbers((int) (first + i)));
                    case PRIMECOUNT -> acc += isPrime(job.getNumbers((int) (first + i))) ? 1 : 0;
                    default -> acc += isPrime(first + i) ? first + i : 0;  // PRIMESUM
                }
                if ((i + 1) % REPORT_EVERY_UNITS == 0) {
                    reporter.report(i + 1);
                }
            }
            reporter.report(count);
            return acc;
        }

        private static long combine(JobSubmission job, List<Future<Long>> parts) throws Exception {
            boolean max = job.getJobType() == common.grpc.client.ClientJobType.MAX;
            long acc = max ? Long.MIN_VALUE : 0;
            for (Future<Long> part : parts) {
                acc = max ? Math.max(acc, part.get()) : acc + part.get();
            }
            return acc;
        }

        private static JobProgress snapshot(JobSubmission job, JobProgress.JobState state, int n,
                                            AtomicLongArray done, long[] total) {
            JobProgress.Builder b = JobProgress.newBuilder().setJobId(job.getJobId()).setState(state)
                    .setCoordinatorId(COORDINATOR_ID);
            for (int w = 0; w < n; w++) {
                long d = done.get(w);
                b.addWorkers(WorkerTaskProgress.newBuilder().setWorkerId(WORKER_IDS[w])
                        .setTaskId(job.getJobId() + "-task-" + (w + 1))
                        .setUnitsCompleted(d).setTotalUnits(total[w])
                        .setStatus(d == total[w] ? "COMPLETED" : "PROCESSING"));
            }
            return b.build();
        }

        private static boolean isPrime(long n) {
            if (n <= 1) return false;
            if (n <= 3) return true;
            if (n % 2 == 0 || n % 3 == 0) return false;
            for (long i = 5; i * i <= n; i += 6) {
                if (n % i == 0 || n % (i + 2) == 0) return false;
            }
            return true;
        }
    }
}
