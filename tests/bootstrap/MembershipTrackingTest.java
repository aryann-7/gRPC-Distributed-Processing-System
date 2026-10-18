package bootstrap;

import common.grpc.CoordinatorRecord;
import common.grpc.MembershipChange;
import common.grpc.MembershipSnapshot;
import common.grpc.MembershipUpdate;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class MembershipTrackingTest {
    private final WorkerRegistry registry = new WorkerRegistry();

    @Test
    public void initialSnapshotIsConsistentAndIndependentOfLaterChanges() throws Exception {
        WorkerDescriptor first = worker(101, "first");
        registry.registerWorker(first);
        try (var watch = registry.watchMembership()) {
            MembershipSnapshot snapshot = watch.nextUpdate().orElseThrow().getSnapshot();
            registry.registerWorker(worker(102, "second"));
            assertEquals(registry.getEpoch(), snapshot.getRegistryEpoch());
            assertEquals(1, snapshot.getMembershipRevision());
            assertEquals(List.of(first), snapshot.getWorkersList());
            assertEquals(2, watch.nextUpdate().orElseThrow().getChange().getMembershipRevision());
            assertEquals(2, registry.membershipSnapshot().getWorkersCount());
        }
    }

    @Test
    public void joinsAndLeavesHaveContiguousRevisionsAndExactDescriptors() throws Exception {
        WorkerDescriptor worker = worker(101, "session");
        try (var watch = registry.watchMembership()) {
            MembershipUpdate initial = watch.nextUpdate().orElseThrow();
            assertTrue(initial.hasSnapshot());
            assertEquals(0, initial.getSnapshot().getMembershipRevision());
            assertTrue(initial.getSnapshot().getWorkersList().isEmpty());
            registry.registerWorker(worker);
            assertChange(watch.nextUpdate().orElseThrow(), MembershipChange.Kind.JOINED, worker, 1);
            registry.deregisterWorker(worker.getIdentity());
            assertChange(watch.nextUpdate().orElseThrow(), MembershipChange.Kind.LEFT, worker, 2);
            assertEquals(2, registry.membershipSnapshot().getMembershipRevision());
            assertTrue(registry.membershipSnapshot().getWorkersList().isEmpty());
        }
    }

    @Test
    public void retriesRejectedOperationsAndCoordinatorUpdatesDoNotEmitChanges() throws Exception {
        WorkerDescriptor worker = worker(101, "session");
        try (var watch = registry.watchMembership()) {
            watch.nextUpdate();
            registry.registerWorker(worker);
            watch.nextUpdate();
            assertFalse(registry.registerWorker(worker));
            assertThrows(IllegalStateException.class, () -> registry.registerWorker(worker(101, "conflict")));
            assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(worker(0, "invalid")));
            assertThrows(IllegalArgumentException.class, () -> registry.deregisterWorker(null));
            assertThrows(IllegalStateException.class, () -> registry.deregisterWorker(worker(101, "stale").getIdentity()));
            assertFalse(registry.deregisterWorker(worker(999, "absent").getIdentity()));
            CoordinatorRecord record = CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(1).build();
            registry.publishCoordinator(record, registry.getEpoch());
            registry.clearCoordinator(worker.getIdentity(), 1, registry.getEpoch());
            registry.deregisterWorker(worker.getIdentity());
            assertFalse(registry.deregisterWorker(worker.getIdentity()));
            registry.registerWorker(worker(102, "next"));
            assertChange(watch.nextUpdate().orElseThrow(), MembershipChange.Kind.LEFT, worker, 2);
            assertChange(watch.nextUpdate().orElseThrow(), MembershipChange.Kind.JOINED, worker(102, "next"), 3);
            assertEquals(3, registry.membershipSnapshot().getMembershipRevision());
        }
    }

    @Test
    public void waitingConsumerObservesRegistration() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var watch = registry.watchMembership()) {
            watch.nextUpdate();
            CountDownLatch reading = new CountDownLatch(1);
            var result = executor.submit(() -> {
                reading.countDown();
                return watch.nextUpdate().orElseThrow();
            });
            assertTrue(reading.await(5, TimeUnit.SECONDS));
            WorkerDescriptor worker = worker(101, "session");
            registry.registerWorker(worker);
            assertChange(result.get(5, TimeUnit.SECONDS), MembershipChange.Kind.JOINED, worker, 1);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void concurrentSubscriptionAndRegistrationNeverLeaveAGap() throws Exception {
        var executor = Executors.newFixedThreadPool(2);
        try {
            for (int id = 101; id < 133; id++) {
                WorkerRegistry concurrent = new WorkerRegistry();
                WorkerDescriptor worker = worker(id, "session");
                CountDownLatch start = new CountDownLatch(1);
                var watching = executor.submit(() -> {
                    start.await();
                    return concurrent.watchMembership();
                });
                var joining = executor.submit(() -> {
                    start.await();
                    return concurrent.registerWorker(worker);
                });
                start.countDown();
                try (var watch = watching.get(5, TimeUnit.SECONDS)) {
                    assertTrue(joining.get(5, TimeUnit.SECONDS));
                    MembershipSnapshot initial = watch.nextUpdate().orElseThrow().getSnapshot();
                    if (initial.getMembershipRevision() == 0) {
                        assertTrue(initial.getWorkersList().isEmpty());
                        var change = executor.submit(() -> watch.nextUpdate().orElseThrow());
                        MembershipChange joined = change.get(5, TimeUnit.SECONDS).getChange();
                        assertEquals(worker, joined.getWorker());
                        assertEquals(1, joined.getMembershipRevision());
                        assertEquals(concurrent.getEpoch(), joined.getRegistryEpoch());
                    } else {
                        assertEquals(1, initial.getMembershipRevision());
                        assertEquals(List.of(worker), initial.getWorkersList());
                    }
                }
            }
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void closingWakesReadersAndDiscardsPendingUpdates() throws Exception {
        var executor = Executors.newSingleThreadExecutor();
        try (var watch = registry.watchMembership()) {
            watch.nextUpdate();
            CountDownLatch reading = new CountDownLatch(1);
            var result = executor.submit(() -> {
                reading.countDown();
                return watch.nextUpdate();
            });
            assertTrue(reading.await(5, TimeUnit.SECONDS));
            watch.close();
            assertTrue(result.get(5, TimeUnit.SECONDS).isEmpty());
            registry.registerWorker(worker(101, "session"));
            assertTrue(watch.nextUpdate().isEmpty());
        } finally {
            executor.shutdownNow();
        }
        var pending = registry.watchMembership();
        pending.close();
        pending.close();
        assertTrue(pending.nextUpdate().isEmpty());
        assertEquals(1, registry.snapshot().size());
    }

    @Test
    public void slowWatcherOverflowsWithoutBlockingOtherWatchersOrMembership() throws Exception {
        try (var slow = registry.watchMembership(); var active = registry.watchMembership()) {
            slow.nextUpdate();
            active.nextUpdate();
            for (int id = 1; id <= 300; id++) {
                WorkerDescriptor worker = worker(id, "session");
                registry.registerWorker(worker);
                assertChange(active.nextUpdate().orElseThrow(), MembershipChange.Kind.JOINED, worker, id);
            }
            assertThrows(IllegalStateException.class, slow::nextUpdate);
            assertEquals(300, registry.membershipSnapshot().getMembershipRevision());
            try (var replacement = registry.watchMembership()) {
                MembershipSnapshot snapshot = replacement.nextUpdate().orElseThrow().getSnapshot();
                assertEquals(300, snapshot.getWorkersCount());
                assertEquals(300, snapshot.getMembershipRevision());
            }
        }
    }

    @Test
    public void pausedWatcherStillDetectsOverflow() throws Exception {
        try (var watch = registry.watchMembership()) {
            watch.setPaused(true);
            for (int id = 1; id <= 300; id++) {
                registry.registerWorker(worker(id, "session"));
            }
            assertThrows(IllegalStateException.class, watch::nextUpdate);
            assertEquals(0, registry.membershipWatchCount());
        }
    }

    private void assertChange(MembershipUpdate update, MembershipChange.Kind kind,
                              WorkerDescriptor worker, long revision) {
        assertTrue(update.hasChange());
        MembershipChange change = update.getChange();
        assertEquals(kind, change.getKind());
        assertEquals(worker, change.getWorker());
        assertEquals(registry.getEpoch(), change.getRegistryEpoch());
        assertEquals(revision, change.getMembershipRevision());
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101)).build();
    }
}
