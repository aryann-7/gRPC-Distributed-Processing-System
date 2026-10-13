package client.progress;

import java.util.List;

/**
 * The full state of a job at one moment, built from one JobProgress message
 * of the coordinator's WatchJob stream.
 *
 * Each message replaces the previous one completely (coordinator.proto:
 * "Later updates replace it"), so the client never adds updates together;
 * it just keeps the newest snapshot.
 *
 * @param queued         true while the coordinator has accepted the job but
 *                       not yet handed any work to workers
 * @param unitsCompleted job-wide units done (from the coordinator)
 * @param totalUnits     job-wide total, including work not yet allocated
 * @param workers        one entry per worker that has received part of the job
 */
public record ProgressSnapshot(boolean queued, long unitsCompleted, long totalUnits,
                               List<WorkerProgress> workers) {

    public ProgressSnapshot {
        workers = (workers == null) ? List.of() : List.copyOf(workers);
    }
}
