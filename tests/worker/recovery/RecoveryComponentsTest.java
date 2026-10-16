
package worker.recovery;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class RecoveryComponentsTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void heartbeatRestoresWorkerToActive() {
        WorkerFailureDetector detector =
                new WorkerFailureDetector(500, 2);

        detector.recordHeartbeat(1, 20);
        assertEquals(
                WorkerFailureDetector.Health.ACTIVE,
                detector.getHealth(1));

        detector.recordMissedHeartbeat(1);
        assertEquals(
                WorkerFailureDetector.Health.SLOW,
                detector.getHealth(1));

        detector.recordMissedHeartbeat(1);
        assertEquals(
                WorkerFailureDetector.Health.SUSPECTED_FAILED,
                detector.getHealth(1));

        detector.recordHeartbeat(1, 25);
        assertEquals(
                WorkerFailureDetector.Health.ACTIVE,
                detector.getHealth(1));
        assertEquals(0, detector.getConsecutiveMisses(1));
    }

    @Test
    void checkpointSurvivesStoreRecreation() throws Exception {
        Path file = temporaryDirectory.resolve("coordinator-state.bin");

        CoordinatorStateStore firstStore =
                new CoordinatorStateStore(file);

        firstStore.save(new CoordinatorStateStore.Snapshot(
                10,
                3,
                2,
                java.util.List.of(
                        new CoordinatorStateStore.TaskSnapshot(
                                "job-1", "task-1", "COMPLETED",
                                4, true, 99))));

        CoordinatorStateStore secondStore =
                new CoordinatorStateStore(file);

        CoordinatorStateStore.Snapshot restored =
                secondStore.load();

        assertNotNull(restored);
        assertEquals(10, restored.coordinatorId());
        assertEquals(3, restored.term());
        assertEquals(1, restored.tasks().size());
        assertEquals("task-1", restored.tasks().get(0).taskId());
        assertEquals(99, restored.tasks().get(0).resultValue());
    }

    @Test
    void completedTaskCannotBeCountedTwice() throws Exception {
        CoordinatorStateStore store =
                new CoordinatorStateStore(
                        temporaryDirectory.resolve("ledger.bin"));

        TaskLedger ledger = new TaskLedger(store, 1, 1, 0);

        assertTrue(ledger.addTask("job-1", "task-1"));
        assertFalse(ledger.addTask("job-1", "task-1"));

        assertTrue(ledger.assignTask("task-1", 7));
        assertTrue(ledger.completeTask("task-1", 7, 42));
        assertFalse(ledger.completeTask("task-1", 7, 42));

        assertEquals(
                TaskLedger.Status.COMPLETED,
                ledger.getTask("task-1").status());
    }

    @Test
    void unfinishedTaskCanBeReassigned() throws Exception {
        CoordinatorStateStore store =
                new CoordinatorStateStore(
                        temporaryDirectory.resolve("reassignment.bin"));

        TaskLedger ledger = new TaskLedger(store, 1, 1, 0);

        ledger.addTask("job-1", "task-1");
        ledger.assignTask("task-1", 5);

        assertEquals(1, ledger.requeueWorkerTasks(5));
        assertEquals(
                TaskLedger.Status.PENDING,
                ledger.getTask("task-1").status());

        assertTrue(ledger.assignTask("task-1", 8));
        assertEquals(8, ledger.getTask("task-1").assignedWorkerId());
    }
}
