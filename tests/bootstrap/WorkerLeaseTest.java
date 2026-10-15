package bootstrap;

import common.grpc.CoordinatorRecord;
import common.grpc.MembershipChange;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerLeaseTest {
    private final AtomicLong now = new AtomicLong();
    private final WorkerRegistry registry = new WorkerRegistry(2, now::get);

    @Test
    public void registrationAndRetriesDoNotEnableExpiryForLegacyWorkers() {
        WorkerDescriptor worker = worker(101, "legacy");
        registry.registerWorker(worker);
        assertFalse(registry.registerWorker(worker));
        now.set(TimeUnit.DAYS.toNanos(365));
        assertEquals(0, registry.expireLeases());
        assertEquals(List.of(worker), registry.snapshot());
        assertEquals(1, registry.membershipSnapshot().getMembershipRevision());
    }

    @Test
    public void activationAndRenewalExtendTheMatchingDeadline() {
        WorkerDescriptor worker = worker(101, "session");
        registry.registerWorker(worker);
        registry.renewLease(worker.getIdentity(), 10);
        advanceMillis(9);
        assertEquals(0, registry.expireLeases());
        registry.renewLease(worker.getIdentity(), 20);
        advanceMillis(19);
        assertEquals(0, registry.expireLeases());
        assertEquals(worker, registry.findWorker(101).orElseThrow());
        advanceMillis(1);
        assertEquals(1, registry.expireLeases());
        assertTrue(registry.findWorker(101).isEmpty());
    }

    @Test
    public void invalidUnknownAndStaleRenewalsCannotActivateOrExtendLeases() {
        WorkerDescriptor worker = worker(101, "current");
        assertThrows(NoSuchElementException.class, () -> registry.renewLease(worker.getIdentity(), 10));
        registry.registerWorker(worker);
        assertThrows(IllegalArgumentException.class, () -> registry.renewLease(null, 10));
        assertThrows(IllegalArgumentException.class,
                () -> registry.renewLease(WorkerIdentity.getDefaultInstance(), 10));
        for (long duration : new long[] { 0, -1, Long.MAX_VALUE }) {
            assertThrows(IllegalArgumentException.class, () -> registry.renewLease(worker.getIdentity(), duration));
        }
        advanceMillis(100);
        assertEquals(0, registry.expireLeases());
        registry.renewLease(worker.getIdentity(), 10);
        advanceMillis(9);
        assertThrows(IllegalStateException.class, () -> registry.renewLease(worker(101, "stale").getIdentity(), 100));
        advanceMillis(1);
        assertEquals(1, registry.expireLeases());
    }

    @Test
    public void exactRegistrationRetryRenewsOnlyAnActiveOptedInLease() {
        WorkerDescriptor worker = worker(101, "session");
        registry.registerWorker(worker);
        registry.renewLease(worker.getIdentity(), 10);
        advanceMillis(9);
        assertFalse(registry.registerWorker(worker));
        advanceMillis(9);
        assertEquals(0, registry.expireLeases());
        assertEquals(1, registry.membershipSnapshot().getMembershipRevision());
        advanceMillis(1);
        assertThrows(IllegalStateException.class, () -> registry.renewLease(worker.getIdentity(), 10));
        assertThrows(IllegalStateException.class, () -> registry.registerWorker(worker));
        assertEquals(1, registry.expireLeases());
    }

    @Test
    public void expiryEmitsOneLeaveAndKeepsUnrelatedWorkersAndSnapshotsConsistent() throws Exception {
        WorkerDescriptor expiring = worker(101, "leased"), legacy = worker(102, "legacy"), live = worker(103, "live");
        registry.registerWorker(expiring);
        registry.registerWorker(legacy);
        registry.registerWorker(live);
        registry.renewLease(expiring.getIdentity(), 10);
        registry.renewLease(live.getIdentity(), 100);
        try (var watch = registry.watchMembership()) {
            assertEquals(3, watch.nextUpdate().orElseThrow().getSnapshot().getMembershipRevision());
            advanceMillis(9);
            registry.renewLease(expiring.getIdentity(), 1);
            assertEquals(3, registry.membershipSnapshot().getMembershipRevision());
            advanceMillis(1);
            assertEquals(1, registry.expireLeases());
            assertEquals(0, registry.expireLeases());
            MembershipChange left = watch.nextUpdate().orElseThrow().getChange();
            assertEquals(MembershipChange.Kind.LEFT, left.getKind());
            assertEquals(expiring, left.getWorker());
            assertEquals(4, left.getMembershipRevision());
            assertEquals(registry.getEpoch(), left.getRegistryEpoch());
            assertEquals(List.of(live), registry.getInitialNeighbours(legacy.getIdentity()));
            assertEquals(2, registry.membershipSnapshot().getWorkersCount());
            assertFalse(registry.membershipSnapshot().getWorkersList().contains(expiring));
            registry.registerWorker(worker(104, "next"));
            MembershipChange joined = watch.nextUpdate().orElseThrow().getChange();
            assertEquals(MembershipChange.Kind.JOINED, joined.getKind());
            assertEquals(5, joined.getMembershipRevision());
        }
    }

    @Test
    public void coordinatorExpiryClearsDirectoryButRetainsElectionTermProtection() {
        WorkerDescriptor old = worker(101, "old");
        registry.registerWorker(old);
        registry.publishCoordinator(CoordinatorRecord.newBuilder().setCoordinator(old).setTerm(3).build(), registry.getEpoch());
        registry.renewLease(old.getIdentity(), 10);
        advanceMillis(10);
        assertEquals(1, registry.expireLeases());
        assertTrue(registry.getCoordinator().isEmpty());
        WorkerDescriptor replacement = worker(101, "new");
        registry.registerWorker(replacement);
        assertThrows(IllegalStateException.class, () -> registry.publishCoordinator(
                CoordinatorRecord.newBuilder().setCoordinator(replacement).setTerm(3).build(), registry.getEpoch()));
        assertTrue(registry.publishCoordinator(CoordinatorRecord.newBuilder()
                .setCoordinator(replacement).setTerm(4).build(), registry.getEpoch()));
    }

    @Test
    public void deregistrationDiscardsLeaseMetadataBeforeTheIdIsReused() {
        WorkerDescriptor old = worker(101, "old"), replacement = worker(101, "new");
        registry.registerWorker(old);
        registry.renewLease(old.getIdentity(), 10);
        registry.deregisterWorker(old.getIdentity());
        registry.registerWorker(replacement);
        advanceMillis(100);
        assertEquals(0, registry.expireLeases());
        assertThrows(IllegalStateException.class, () -> registry.renewLease(old.getIdentity(), 10));
        assertEquals(replacement, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void monotonicDeadlineWorksAcrossSignedNanoTimeWraparound() {
        now.set(Long.MAX_VALUE - TimeUnit.MILLISECONDS.toNanos(5));
        WorkerDescriptor worker = worker(101, "session");
        registry.registerWorker(worker);
        registry.renewLease(worker.getIdentity(), 10);
        advanceMillis(9);
        assertEquals(0, registry.expireLeases());
        advanceMillis(1);
        assertEquals(1, registry.expireLeases());
    }

    @Test
    public void concurrentExpiryAndLateRenewalCannotRemoveAReplacementSession() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            for (int id = 101; id < 117; id++) {
                WorkerDescriptor old = worker(id, "old"), replacement = worker(id, "new");
                registry.registerWorker(old);
                registry.renewLease(old.getIdentity(), 10);
                advanceMillis(10);
                CountDownLatch start = new CountDownLatch(1);
                var removal = executor.submit(() -> {
                    start.await();
                    int removed = registry.expireLeases();
                    registry.registerWorker(replacement);
                    return removed;
                });
                var renewal = executor.submit(() -> {
                    start.await();
                    try {
                        registry.renewLease(old.getIdentity(), 100);
                        return true;
                    } catch (IllegalStateException | NoSuchElementException expected) {
                        return false;
                    }
                });
                start.countDown();
                assertEquals(1, removal.get(5, TimeUnit.SECONDS));
                assertFalse(renewal.get(5, TimeUnit.SECONDS));
                assertEquals(replacement, registry.findWorker(id).orElseThrow());
                assertEquals(0, registry.expireLeases());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private void advanceMillis(long millis) {
        now.addAndGet(TimeUnit.MILLISECONDS.toNanos(millis));
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101)).build();
    }
}
