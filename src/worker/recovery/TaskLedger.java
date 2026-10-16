
package worker.recovery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Tracks task assignment, completion and reassignment.
 *
 * The ledger prevents a completed task from being counted twice.
 */
public class TaskLedger {

    public enum Status {
        PENDING,
        RUNNING,
        COMPLETED
    }

    public record TaskRecord(
            String jobId,
            String taskId,
            Status status,
            int assignedWorkerId,
            boolean hasResult,
            long resultValue) {
    }

    private final Map<String, TaskRecord> tasks =
            new LinkedHashMap<>();

    private final CoordinatorStateStore stateStore;

    private int coordinatorId;
    private int term;
    private int jac;

    public TaskLedger(
            CoordinatorStateStore stateStore,
            int coordinatorId,
            int term,
            int jac) {

        if (stateStore == null) {
            throw new IllegalArgumentException(
                    "State store cannot be null");
        }

        this.stateStore = stateStore;
        this.coordinatorId = coordinatorId;
        this.term = term;
        this.jac = jac;
    }

    /**
     * Adds a task only if its ID is not already present.
     */
    public synchronized boolean addTask(
            String jobId,
            String taskId) throws IOException {

        validateId(jobId, "jobId");
        validateId(taskId, "taskId");

        if (tasks.containsKey(taskId)) {
            return false;
        }

        tasks.put(taskId, new TaskRecord(
                jobId, taskId, Status.PENDING,
                -1, false, 0));

        persist();
        return true;
    }

    /**
     * Assigns a pending task to a worker.
     */
    public synchronized boolean assignTask(
            String taskId,
            int workerId) throws IOException {

        if (workerId < 0) {
            throw new IllegalArgumentException(
                    "Worker ID cannot be negative");
        }

        TaskRecord task = tasks.get(taskId);

        if (task == null || task.status() != Status.PENDING) {
            return false;
        }

        tasks.put(taskId, new TaskRecord(
                task.jobId(), task.taskId(), Status.RUNNING,
                workerId, false, 0));

        persist();
        return true;
    }

    /**
     * Records the first successful result for a task.
     * Later results for that task are ignored.
     */
    public synchronized boolean completeTask(
            String taskId,
            int workerId,
            long resultValue) throws IOException {

        TaskRecord task = tasks.get(taskId);

        if (task == null || task.status() == Status.COMPLETED) {
            return false;
        }

        if (task.assignedWorkerId() != workerId) {
            return false;
        }

        tasks.put(taskId, new TaskRecord(
                task.jobId(), task.taskId(), Status.COMPLETED,
                workerId, true, resultValue));

        persist();
        return true;
    }

    /**
     * Returns unfinished tasks assigned to a failed worker to PENDING.
     */
    public synchronized int requeueWorkerTasks(
            int failedWorkerId) throws IOException {

        int requeued = 0;

        for (Map.Entry<String, TaskRecord> entry
                : new ArrayList<>(tasks.entrySet())) {

            TaskRecord task = entry.getValue();

            if (task.status() == Status.RUNNING
                    && task.assignedWorkerId() == failedWorkerId) {

                tasks.put(entry.getKey(), new TaskRecord(
                        task.jobId(), task.taskId(), Status.PENDING,
                        -1, false, 0));

                requeued++;
            }
        }

        if (requeued > 0) {
            persist();
        }

        return requeued;
    }

    public synchronized TaskRecord getTask(String taskId) {
        return tasks.get(taskId);
    }

    public synchronized Collection<TaskRecord> getAllTasks() {
        return List.copyOf(tasks.values());
    }

    public synchronized List<TaskRecord> getPendingTasks() {
        return tasks.values().stream()
                .filter(task -> task.status() == Status.PENDING)
                .toList();
    }

    public synchronized void updateCoordinator(
            int newCoordinatorId,
            int newTerm,
            int newJac) throws IOException {

        if (newTerm < term) {
            throw new IllegalArgumentException(
                    "Cannot move to an older coordinator term");
        }

        coordinatorId = newCoordinatorId;
        term = newTerm;
        jac = newJac;
        persist();
    }

    /**
     * Restores task records from a checkpoint.
     * Tasks that were RUNNING become PENDING because the old
     * coordinator may have failed before recording their results.
     */
    public synchronized void restore() throws IOException {
        CoordinatorStateStore.Snapshot snapshot = stateStore.load();

        if (snapshot == null) {
            return;
        }

        coordinatorId = snapshot.coordinatorId();
        term = snapshot.term();
        jac = snapshot.jac();

        tasks.clear();

        for (CoordinatorStateStore.TaskSnapshot saved
                : snapshot.tasks()) {

            Status status;

            try {
                status = Status.valueOf(saved.status());
            } catch (IllegalArgumentException exception) {
                throw new IOException(
                        "Invalid saved task status: " + saved.status(),
                        exception);
            }

            if (status == Status.RUNNING) {
                status = Status.PENDING;
            }

            int workerId = status == Status.COMPLETED
                    ? saved.assignedWorkerId()
                    : -1;

            tasks.put(saved.taskId(), new TaskRecord(
                    saved.jobId(),
                    saved.taskId(),
                    status,
                    workerId,
                    status == Status.COMPLETED && saved.hasResult(),
                    saved.resultValue()));
        }
    }

    private void persist() throws IOException {
        List<CoordinatorStateStore.TaskSnapshot> savedTasks =
                new ArrayList<>();

        for (TaskRecord task : tasks.values()) {
            savedTasks.add(new CoordinatorStateStore.TaskSnapshot(
                    task.jobId(),
                    task.taskId(),
                    task.status().name(),
                    task.assignedWorkerId(),
                    task.hasResult(),
                    task.resultValue()));
        }

        stateStore.save(new CoordinatorStateStore.Snapshot(
                coordinatorId, term, jac, savedTasks));
    }

    private static void validateId(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    name + " cannot be empty");
        }
    }
}
