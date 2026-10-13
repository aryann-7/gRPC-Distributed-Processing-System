package bootstrap;

import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerRegistryTest {

    @Test
    public void registersAndLooksUpTypedWorkers() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor worker = worker(101, "session-1");
        assertTrue(registry.registerWorker(worker));
        assertEquals(worker, registry.findWorker(101).orElseThrow());
        assertTrue(registry.findWorker(999).isEmpty());
        assertEquals(List.of(worker), registry.snapshot());
        assertThrows(IllegalArgumentException.class, () -> registry.findWorker(0));
    }

    @Test
    public void exactRetriesPreserveRegistration() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor worker = worker(101, "session-1");
        registry.registerWorker(worker);
        assertFalse(registry.registerWorker(worker.toBuilder().build()));
        assertEquals(List.of(worker), registry.snapshot());
    }

    @Test
    public void conflictingSessionsAndEndpointsCannotReplaceWorker() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor worker = worker(101, "session-1");
        registry.registerWorker(worker);
        assertThrows(IllegalStateException.class, () -> registry.registerWorker(worker(101, "session-2")));
        assertThrows(IllegalStateException.class, () -> registry.registerWorker(worker.toBuilder()
                .setEndpoint(worker.getEndpoint().toBuilder().setPort(1201)).build()));
        assertThrows(IllegalStateException.class, () -> registry.registerWorker(worker.toBuilder()
                .setEndpoint(worker.getEndpoint().toBuilder().setHost("other-host")).build()));
        assertEquals(worker, registry.findWorker(101).orElseThrow());
    }

    @Test
    public void invalidIdentitiesAndMissingMessagesLeaveRegistryEmpty() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor valid = worker(101, "session-1");
        assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(null));
        assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(valid.toBuilder().clearIdentity().build()));
        assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(valid.toBuilder().clearEndpoint().build()));
        for (int id : new int[] { 0, -1, Integer.MIN_VALUE }) {
            assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(worker(id, "session-1")));
        }
        for (String session : new String[] { "", " \t " }) {
            assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(worker(101, session)));
        }
        assertTrue(registry.snapshot().isEmpty());
    }

    @Test
    public void validatesEndpointsAndAcceptsBoundaryValues() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor valid = worker(101, "session-1");
        for (int port : new int[] { -1, 0, 65536 }) {
            assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(valid.toBuilder()
                    .setEndpoint(valid.getEndpoint().toBuilder().setPort(port)).build()));
        }
        for (String host : new String[] { "", " ", "bad host", "host\tname", "http://localhost",
                "localhost:1101", "host/path", "host?query", "host#fragment", "user@host",
                "[::1]", "0.0.0.0", "::", "0:0:0:0:0:0:0:0" }) {
            assertThrows(IllegalArgumentException.class, () -> registry.registerWorker(valid.toBuilder()
                    .setEndpoint(valid.getEndpoint().toBuilder().setHost(host)).build()), host);
        }
        assertTrue(registry.snapshot().isEmpty());
        registry.registerWorker(worker(1, "min").toBuilder().setEndpoint(
                WorkerEndpoint.newBuilder().setHost("127.0.0.1").setPort(1)).build());
        registry.registerWorker(worker(Integer.MAX_VALUE, "max").toBuilder().setEndpoint(
                WorkerEndpoint.newBuilder().setHost("2001:db8::1").setPort(65535)).build());
        assertEquals(2, registry.snapshot().size());
    }

    @Test
    public void snapshotsAreImmutableAndIndependentOfLaterChanges() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor worker = worker(101, "session-1");
        registry.registerWorker(worker);
        List<WorkerDescriptor> snapshot = registry.snapshot();
        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(worker(102, "session-2")));
        assertThrows(UnsupportedOperationException.class, snapshot::clear);
        registry.deregisterWorker(worker.getIdentity());
        assertEquals(List.of(worker), snapshot);
        assertTrue(registry.snapshot().isEmpty());
    }

    @Test
    public void deregistrationIsIdempotentAndRejectsStaleSessions() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor original = worker(101, "session-1");
        registry.registerWorker(original);
        assertTrue(registry.deregisterWorker(original.getIdentity()));
        assertFalse(registry.deregisterWorker(original.getIdentity()));
        WorkerDescriptor replacement = worker(101, "session-2");
        assertTrue(registry.registerWorker(replacement));
        assertThrows(IllegalStateException.class, () -> registry.deregisterWorker(original.getIdentity()));
        assertThrows(IllegalStateException.class, () -> registry.getInitialNeighbours(original.getIdentity()));
        assertEquals(replacement, registry.findWorker(101).orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> registry.deregisterWorker(null));
        assertThrows(IllegalArgumentException.class, () -> registry.deregisterWorker(WorkerIdentity.getDefaultInstance()));
        assertTrue(registry.deregisterWorker(replacement.getIdentity()));
    }

    @Test
    public void firstWorkerHasNoNeighboursAndUnknownWorkersCannotDiscover() {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor worker = worker(101, "session-1");
        assertThrows(NoSuchElementException.class, () -> registry.getInitialNeighbours(worker.getIdentity()));
        registry.registerWorker(worker);
        assertTrue(registry.getInitialNeighbours(worker.getIdentity()).isEmpty());
        assertThrows(IllegalArgumentException.class, () -> registry.getInitialNeighbours(null));
    }

    @Test
    public void neighboursExcludeSelfDuplicatesAndRemovedWorkers() {
        WorkerRegistry registry = new WorkerRegistry(2);
        for (int id = 101; id <= 105; id++) {
            registry.registerWorker(worker(id, "session-" + id));
        }
        registry.deregisterWorker(worker(105, "session-105").getIdentity());
        for (int trial = 0; trial < 50; trial++) {
            List<WorkerDescriptor> peers = registry.getInitialNeighbours(worker(101, "session-101").getIdentity());
            assertEquals(2, peers.size());
            assertEquals(2L, peers.stream().map(p -> p.getIdentity().getWorkerId()).distinct().count());
            assertTrue(peers.stream().allMatch(p -> p.getIdentity().getWorkerId() >= 102
                    && p.getIdentity().getWorkerId() <= 104));
            assertThrows(UnsupportedOperationException.class, peers::clear);
        }
    }

    @Test
    public void neighbourLimitIsBoundedByAvailablePeers() {
        for (int limit : new int[] { -1, 0, 1, 10 }) {
            WorkerRegistry registry = new WorkerRegistry(limit);
            for (int id = 101; id <= 103; id++) {
                registry.registerWorker(worker(id, "session-" + id));
            }
            assertEquals(Math.min(Math.max(1, limit), 2),
                    registry.getInitialNeighbours(worker(101, "session-101").getIdentity()).size());
        }
    }

    @Test
    public void concurrentRetriesCreateOnlyOneRegistration() throws Exception {
        WorkerRegistry registry = new WorkerRegistry();
        WorkerDescriptor worker = worker(101, "session-1");
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<Boolean>> results = new ArrayList<>();
            for (int i = 0; i < 32; i++) {
                results.add(executor.submit(() -> {
                    start.await();
                    return registry.registerWorker(worker);
                }));
            }
            start.countDown();
            int created = 0;
            for (Future<Boolean> result : results) {
                if (result.get(5, TimeUnit.SECONDS)) {
                    created++;
                }
            }
            assertEquals(1, created);
            assertEquals(List.of(worker), registry.snapshot());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void concurrentRegistrationSnapshotsAndRemovalDoNotLoseWorkers() throws Exception {
        WorkerRegistry registry = new WorkerRegistry();
        var executor = Executors.newFixedThreadPool(8);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<?>> results = new ArrayList<>();
            for (int id = 101; id <= 132; id++) {
                WorkerDescriptor worker = worker(id, "session-" + id);
                results.add(executor.submit(() -> {
                    start.await();
                    assertTrue(registry.registerWorker(worker));
                    assertEquals(worker, registry.findWorker(worker.getIdentity().getWorkerId()).orElseThrow());
                    List<WorkerDescriptor> snapshot = registry.snapshot();
                    assertEquals(snapshot.size(), snapshot.stream().map(p -> p.getIdentity().getWorkerId()).distinct().count());
                    assertTrue(registry.deregisterWorker(worker.getIdentity()));
                    return null;
                }));
            }
            start.countDown();
            for (Future<?> result : results) {
                result.get(5, TimeUnit.SECONDS);
            }
            assertTrue(registry.snapshot().isEmpty());
        } finally {
            executor.shutdownNow();
        }
    }

    private static WorkerDescriptor worker(int id, String session) {
        return WorkerDescriptor.newBuilder()
                .setIdentity(WorkerIdentity.newBuilder().setWorkerId(id).setSessionId(session))
                .setEndpoint(WorkerEndpoint.newBuilder().setHost("localhost").setPort(1101))
                .build();
    }
}
