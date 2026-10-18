package bootstrap;

import common.grpc.CoordinatorRecord;
import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorDirectoryTest {
    private final WorkerRegistry registry = new WorkerRegistry();

    @Test
    public void absentDirectoryAndRepeatedClearingAreSafe() {
        assertTrue(registry.getCoordinator().isEmpty());
        assertFalse(registry.clearCoordinator(worker(101, "session").getIdentity(), 1, registry.getEpoch()));
    }

    @Test
    public void validPublicationAndExactRetryPreserveMembershipRevision() {
        WorkerDescriptor worker = worker(101, "session");
        registry.registerWorker(worker);
        CoordinatorRecord record = record(worker, 2);
        assertTrue(registry.publishCoordinator(record, registry.getEpoch()));
        assertFalse(registry.publishCoordinator(record.toBuilder().build(), registry.getEpoch()));
        assertEquals(record, registry.getCoordinator().orElseThrow());
        assertEquals(1, registry.membershipSnapshot().getMembershipRevision());
    }

    @Test
    public void invalidPublicationsDoNotChangeState() {
        assertThrows(IllegalArgumentException.class, () -> registry.publishCoordinator(null, registry.getEpoch()));
        assertThrows(IllegalArgumentException.class,
                () -> registry.publishCoordinator(CoordinatorRecord.getDefaultInstance(), registry.getEpoch()));
        for (int term : new int[] { -1, 0 }) {
            assertThrows(IllegalArgumentException.class,
                    () -> registry.publishCoordinator(record(worker(101, "session"), term), registry.getEpoch()));
        }
        assertThrows(IllegalArgumentException.class,
                () -> registry.publishCoordinator(record(worker(0, "session"), 1), registry.getEpoch()));
        assertTrue(registry.getCoordinator().isEmpty());
    }

    @Test
    public void unregisteredStaleAndChangedDescriptorsCannotPublish() {
        WorkerDescriptor current = worker(101, "new-session");
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(current, 1), registry.getEpoch()));
        registry.registerWorker(current);
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(worker(101, "old-session"), 1), registry.getEpoch()));
        WorkerDescriptor changed = current.toBuilder()
                .setEndpoint(current.getEndpoint().toBuilder().setPort(1201)).build();
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(changed, 1), registry.getEpoch()));
        assertTrue(registry.publishCoordinator(record(current, 1), registry.getEpoch()));
    }

    @Test
    public void onlyNewerTermsCanReplaceCoordinator() {
        WorkerDescriptor first = worker(101, "first"), second = worker(102, "second");
        registry.registerWorker(first);
        registry.registerWorker(second);
        registry.publishCoordinator(record(first, 2), registry.getEpoch());
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(second, 2), registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(second, 1), registry.getEpoch()));
        assertTrue(registry.publishCoordinator(record(second, 3), registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(first, 2), registry.getEpoch()));
        assertEquals(record(second, 3), registry.getCoordinator().orElseThrow());
    }

    @Test
    public void clearingRequiresMatchingIdentitySessionTermAndEpoch() {
        WorkerDescriptor worker = worker(101, "current");
        registry.registerWorker(worker);
        registry.publishCoordinator(record(worker, 3), registry.getEpoch());
        assertThrows(IllegalStateException.class,
                () -> registry.clearCoordinator(worker(102, "current").getIdentity(), 3, registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.clearCoordinator(worker(101, "old").getIdentity(), 3, registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.clearCoordinator(worker.getIdentity(), 2, registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.clearCoordinator(worker.getIdentity(), 3, "old-epoch"));
        assertTrue(registry.clearCoordinator(worker.getIdentity(), 3, registry.getEpoch()));
        assertFalse(registry.clearCoordinator(worker.getIdentity(), 3, registry.getEpoch()));
        assertEquals(1, registry.membershipSnapshot().getMembershipRevision());
    }

    @Test
    public void clearedCoordinatorCannotBeResurrectedAtAnOldTerm() {
        WorkerDescriptor worker = worker(101, "session");
        registry.registerWorker(worker);
        registry.publishCoordinator(record(worker, 3), registry.getEpoch());
        registry.clearCoordinator(worker.getIdentity(), 3, registry.getEpoch());
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(worker, 3), registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(worker, 2), registry.getEpoch()));
        assertTrue(registry.publishCoordinator(record(worker, 4), registry.getEpoch()));
    }

    @Test
    public void deregistrationInvalidatesOnlyTheMatchingCoordinator() {
        WorkerDescriptor coordinator = worker(101, "old"), peer = worker(102, "peer");
        registry.registerWorker(coordinator);
        registry.registerWorker(peer);
        registry.publishCoordinator(record(coordinator, 3), registry.getEpoch());
        registry.deregisterWorker(peer.getIdentity());
        assertTrue(registry.getCoordinator().isPresent());
        assertThrows(IllegalStateException.class, () -> registry.deregisterWorker(worker(101, "stale").getIdentity()));
        assertTrue(registry.getCoordinator().isPresent());
        registry.deregisterWorker(coordinator.getIdentity());
        assertTrue(registry.getCoordinator().isEmpty());
        WorkerDescriptor restarted = worker(101, "new");
        registry.registerWorker(restarted);
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(coordinator, 4), registry.getEpoch()));
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(restarted, 3), registry.getEpoch()));
        assertTrue(registry.publishCoordinator(record(restarted, 4), registry.getEpoch()));
    }

    @Test
    public void staleEpochPublicationCannotReplaceCoordinator() {
        WorkerDescriptor worker = worker(101, "session");
        registry.registerWorker(worker);
        registry.publishCoordinator(record(worker, 2), registry.getEpoch());
        assertThrows(IllegalStateException.class,
                () -> registry.publishCoordinator(record(worker, 3), "old-epoch"));
        assertEquals(2, registry.getCoordinator().orElseThrow().getTerm());
    }

    @Test
    public void concurrentPublicationAndRemovalCannotLeaveAnUnregisteredCoordinator() throws Exception {
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int id = 101; id <= 132; id++) {
                WorkerDescriptor worker = worker(id, "session-" + id);
                int term = id;
                results.add(executor.submit(() -> {
                    start.await();
                    registry.registerWorker(worker);
                    try {
                        registry.publishCoordinator(record(worker, term), registry.getEpoch());
                    } catch (IllegalStateException expected) {
                        // A higher term may have won the publication race.
                    }
                    registry.deregisterWorker(worker.getIdentity());
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> result : results) {
                result.get(5, TimeUnit.SECONDS);
            }
            assertTrue(registry.snapshot().isEmpty());
            assertTrue(registry.getCoordinator().isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    private static CoordinatorRecord record(WorkerDescriptor worker, int term) {
        return CoordinatorRecord.newBuilder().setCoordinator(worker).setTerm(term).build();
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder().setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101)).build();
    }
}
