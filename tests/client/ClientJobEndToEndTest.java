package client;

import bootstrap.BootstrapServiceImpl;
import common.interfaces.CoordinatorService;
import common.models.JobRequest;
import common.models.JobResult;
import common.models.JobType;
import java.net.ServerSocket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import worker.WorkerNode;

/**
 * End-to-end integration test for Section 4.
 *
 * Spins up a real Bootstrap Node and 3 real WorkerNode instances actual
 * RMI objects making actual network calls, nothing mocked  triggers a
 * genuine leader election, then exercises exactly the same discovery and
 * submission path ClientGuiFrame uses:
 *
 *   1. One "client" submits several jobs at once through a thread pool
 *      (mirrors clientSubmissionPool) and checks every result — validates
 *      concurrent submission from a single client.
 *   2. Several independent RMI lookups (standing in for separate client
 *      processes — RMI can't tell the difference) submit jobs to the
 *      coordinator at the same time — validates multiple clients hitting
 *      the same coordinator simultaneously.
 *
 * Run this in isolation — not while scripts/run_cluster.bat is also
 * running. WorkerNode.discoverClusterWorkers() unconditionally scans
 * ports 1101-1110 on top of its normal neighbour list, so a live cluster
 * and this test's cluster could see each other's workers. This test uses
 * OS-assigned ephemeral ports and worker IDs (900+) specifically to stay
 * out of that range, but the scan itself isn't test-scoped.
 */
public class ClientJobEndToEndTest implements AutoCloseable {

    private static final int WORKER_COUNT = 3;
    private static final long ELECTION_TIMEOUT_MS = 8_000;

    private final List<Remote> exported = new ArrayList<>();
    private int bootstrapPort;

    public static void main(String[] args) throws Exception {
        try (ClientJobEndToEndTest test = new ClientJobEndToEndTest()) {
            CoordinatorService coordinator = test.startClusterAndElectCoordinator();
            test.testSingleClientConcurrentJobs(coordinator);
            test.testMultipleClientsSimultaneously();
        }
        System.out.println("All end-to-end client/coordinator integration tests passed.");
    }

    
    // Cluster bring-up: bootstrap + 3 workers + a real election
    

    private CoordinatorService startClusterAndElectCoordinator() throws Exception {
        bootstrapPort = registry();
        BootstrapServiceImpl bootstrap = track(new BootstrapServiceImpl());
        LocateRegistry.getRegistry("localhost", bootstrapPort).rebind("BootstrapService", bootstrap);
        System.out.println("PASS: bootstrap started on ephemeral port " + bootstrapPort);

        List<WorkerNode> workers = new ArrayList<>();
        for (int i = 0; i < WORKER_COUNT; i++) {
            int workerId = 900 + i; // deliberately outside the 101-110
                                     // range used by run_cluster.bat
            int port = registry();
            WorkerNode worker = track(new WorkerNode(workerId, "localhost", port));
            LocateRegistry.getRegistry("localhost", port).rebind("Worker-" + workerId, worker);
            worker.connectToBootstrap("localhost", bootstrapPort);
            workers.add(worker);
        }
        System.out.println("PASS: " + WORKER_COUNT + " workers registered with bootstrap");

        // Any single worker can start the election — message flooding and
        // duplicate-ID suppression handle propagating it to the rest.
        workers.get(0).startElection();

        int coordinatorId = waitForElectionToSettle(workers);
        System.out.println("PASS: election settled — all workers agree coordinator is Worker " + coordinatorId);

        // Look the coordinator up exactly the way ClientGuiFrame does:
        // LocateRegistry against the known host/port, then lookup the
        // fixed name "CoordinatorService" — never reach into a specific
        // worker directly, since the client can't know in advance which
        // worker won.
        Registry bootstrapRegistry = LocateRegistry.getRegistry("localhost", bootstrapPort);
        CoordinatorService coordinator = (CoordinatorService) bootstrapRegistry.lookup("CoordinatorService");
        check(coordinator != null, "CoordinatorService must be discoverable via the bootstrap registry, same as the real client");
        System.out.println("PASS: CoordinatorService discoverable via bootstrap registry (the client's exact lookup path)");

        return coordinator;
    }

    private int waitForElectionToSettle(List<WorkerNode> workers) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ELECTION_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            int firstCoordinatorId = workers.get(0).getCoordinatorId();
            boolean allAgree = firstCoordinatorId != -1;
            for (WorkerNode w : workers) {
                if (w.getCoordinatorId() != firstCoordinatorId) {
                    allAgree = false;
                    break;
                }
            }
            if (allAgree) {
                long coordinatorCount = workers.stream().filter(WorkerNode::isCoordinator).count();
                check(coordinatorCount == 1, "Exactly one worker must consider itself the coordinator, found " + coordinatorCount);
                return firstCoordinatorId;
            }
            Thread.sleep(100);
        }
        throw new AssertionError("Election did not settle within " + ELECTION_TIMEOUT_MS + "ms");
    }

    
    // Requirement: single client, multiple concurrent tasks via RMI
    
    private void testSingleClientConcurrentJobs(CoordinatorService coordinator) throws Exception {
        // Mirrors ClientGuiFrame.clientSubmissionPool exactly: a cached
        // thread pool so the caller never blocks waiting on an RMI call.
        ExecutorService clientSubmissionPool = Executors.newCachedThreadPool();

        Future<JobResult> maxFuture = submitAsync(clientSubmissionPool, coordinator,
                new JobRequest("t1", JobType.MAX, List.of(3L, 99L, 7L, 42L)));
        Future<JobResult> primeSumFuture = submitAsync(clientSubmissionPool, coordinator,
                new JobRequest("t2", JobType.PRIMESUM, 1, 20));
        Future<JobResult> primeCountFuture = submitAsync(clientSubmissionPool, coordinator,
                new JobRequest("t3", JobType.PRIMECOUNT, List.of(2L, 3L, 4L, 5L, 9L, 11L)));

        JobResult maxResult = maxFuture.get(15, TimeUnit.SECONDS);
        JobResult primeSumResult = primeSumFuture.get(15, TimeUnit.SECONDS);
        JobResult primeCountResult = primeCountFuture.get(15, TimeUnit.SECONDS);

        check(maxResult.isSuccess() && maxResult.getResultValue() == 99,
                "MAX of [3,99,7,42] must be 99, got " + maxResult.getResultValue());
        check(primeSumResult.isSuccess() && primeSumResult.getResultValue() == 77,
                "Sum of primes 1..20 must be 77 (2+3+5+7+11+13+17+19), got " + primeSumResult.getResultValue());
        check(primeCountResult.isSuccess() && primeCountResult.getResultValue() == 4,
                "PRIMECOUNT of [2,3,4,5,9,11] must be 4 (2,3,5,11 are prime), got " + primeCountResult.getResultValue());

        clientSubmissionPool.shutdown();
        System.out.println("PASS: single client submitted 3 jobs concurrently via a thread pool, all results correct");
    }

    private Future<JobResult> submitAsync(ExecutorService pool, CoordinatorService coordinator, JobRequest request) {
        return pool.submit((Callable<JobResult>) () -> coordinator.submitJob(request));
    }

    
    // Requirement: multiple distinct clients hitting the coordinator
    // at the same time
  
    private void testMultipleClientsSimultaneously() throws Exception {
        int clientCount = 3;
        ExecutorService clients = Executors.newFixedThreadPool(clientCount);
        List<Future<JobResult>> futures = new ArrayList<>();

        for (int i = 0; i < clientCount; i++) {
            long start = 1 + (i * 100L);
            long end = start + 49;
            futures.add(clients.submit(() -> submitWithRetryOnTermExpiry(
                    new JobRequest("multi-" + start, JobType.PRIMESUM, start, end))));
        }

        for (Future<JobResult> future : futures) {
            JobResult result = future.get(20, TimeUnit.SECONDS);
            check(result != null && result.isSuccess(),
                    "Every simultaneous client submission must succeed (after retrying past any mid-test coordinator re-election): "
                    + (result == null ? "null result" : result.getErrorMessage()));
    }

        clients.shutdown();
        System.out.println("PASS: " + clientCount + " independent client lookups submitted jobs to the same coordinator at once, all succeeded");
    }

    /**
     * The 5-job term limit means a coordinator can legitimately expire and
     * trigger a new election WHILE a job is in flight — exactly what real,
     * independent client processes must tolerate too. Each simulated
     * client here does its own fresh lookup and retries if it catches the
     * coordinator mid-rotation, rather than treating that as a failure.
     */
    private JobResult submitWithRetryOnTermExpiry(JobRequest request) throws Exception {
        JobResult lastResult = null;
        Exception lastError = null;

        for (int attempt = 1; attempt <= 5; attempt++) {
            try {
                Registry reg = LocateRegistry.getRegistry("localhost", bootstrapPort);
                CoordinatorService coord = (CoordinatorService) reg.lookup("CoordinatorService");
                lastResult = coord.submitJob(request);

                if (lastResult != null && lastResult.isSuccess()) {
                    return lastResult;
                }

                System.out.println("Submission for " + request.getJobId()
                        + " hit a coordinator in transition ("
                        + (lastResult != null ? lastResult.getErrorMessage() : "no result")
                        + "), retrying...");

            } catch (Exception e) {
                // Covers the gap between the old coordinator unbinding and
                // the new one binding - lookup() itself can throw
                // NotBoundException here, not just return an unsuccessful
                // JobResult. Treat it the same way: wait and retry.
                lastError = e;
                System.out.println("Submission for " + request.getJobId()
                        + " caught " + e.getClass().getSimpleName()
                        + " (coordinator likely mid-election), retrying...");
            }

            Thread.sleep(1500);
        }

        if (lastResult != null) {
            return lastResult;
        }
        throw new AssertionError("Gave up after 5 attempts for " + request.getJobId(), lastError);
    }

    
    // Test plumbing — same pattern as WorkerBootstrapIntegrationTest
    
    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    private <T extends Remote> T track(T object) {
        exported.add(object);
        return object;
    }

    private int registry() throws RemoteException {
        int[] port = new int[1];
        track(LocateRegistry.createRegistry(0, null, requestedPort -> {
            ServerSocket socket = new ServerSocket(requestedPort);
            port[0] = socket.getLocalPort();
            return socket;
        }));
        return port[0];
    }

    @Override
    public void close() {
        for (int i = exported.size() - 1; i >= 0; i--) {
            try {
                UnicastRemoteObject.unexportObject(exported.get(i), true);
            } catch (Exception ignored) {
                // best-effort cleanup; a failure here shouldn't mask a
                // real test failure that already happened above
            }
        }
    }
}