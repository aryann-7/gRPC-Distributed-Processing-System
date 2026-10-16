
package worker.recovery;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Tracks heartbeat observations for worker nodes.
 *
 * A missed heartbeat first marks a worker as SUSPECTED_FAILED.
 * It does not immediately prove that the worker has crashed.
 */
public class WorkerFailureDetector {

    public enum Health {
        ACTIVE,
        SLOW,
        SUSPECTED_FAILED
    }

    private static final class Observation {
        private volatile long lastResponseNanos;
        private volatile long lastRoundTripMillis;
        private volatile int consecutiveMisses;
        private volatile Health health;

        private Observation() {
            this.lastResponseNanos = System.nanoTime();
            this.lastRoundTripMillis = 0;
            this.consecutiveMisses = 0;
            this.health = Health.ACTIVE;
        }
    }

    private final ConcurrentMap<Integer, Observation> observations =
            new ConcurrentHashMap<>();

    private final long slowResponseMillis;
    private final int missesBeforeSuspectedFailure;

    public WorkerFailureDetector(
            long slowResponseMillis,
            int missesBeforeSuspectedFailure) {

        if (slowResponseMillis <= 0) {
            throw new IllegalArgumentException(
                    "slowResponseMillis must be positive");
        }

        if (missesBeforeSuspectedFailure < 1) {
            throw new IllegalArgumentException(
                    "missesBeforeSuspectedFailure must be at least 1");
        }

        this.slowResponseMillis = slowResponseMillis;
        this.missesBeforeSuspectedFailure =
                missesBeforeSuspectedFailure;
    }

    /**
     * Record a successful heartbeat response.
     */
    public void recordHeartbeat(
            int workerId,
            long roundTripMillis) {

        if (workerId < 0 || roundTripMillis < 0) {
            throw new IllegalArgumentException(
                    "Invalid worker ID or response time");
        }

        Observation observation = observations.computeIfAbsent(
                workerId, ignored -> new Observation());

        synchronized (observation) {
            observation.lastResponseNanos = System.nanoTime();
            observation.lastRoundTripMillis = roundTripMillis;
            observation.consecutiveMisses = 0;
            observation.health =
                    roundTripMillis > slowResponseMillis
                            ? Health.SLOW
                            : Health.ACTIVE;
        }
    }

    /**
     * Record one missed heartbeat attempt.
     */
    public void recordMissedHeartbeat(int workerId) {
        if (workerId < 0) {
            throw new IllegalArgumentException("Invalid worker ID");
        }

        Observation observation = observations.computeIfAbsent(
                workerId, ignored -> new Observation());

        synchronized (observation) {
            observation.consecutiveMisses++;

            if (observation.consecutiveMisses
                    >= missesBeforeSuspectedFailure) {
                observation.health = Health.SUSPECTED_FAILED;
            } else {
                observation.health = Health.SLOW;
            }
        }
    }

    /**
     * Returns the last recorded health classification.
     */
    public Health getHealth(int workerId) {
        Observation observation = observations.get(workerId);

        return observation == null
                ? Health.SUSPECTED_FAILED
                : observation.health;
    }

    public int getConsecutiveMisses(int workerId) {
        Observation observation = observations.get(workerId);

        return observation == null
                ? 0
                : observation.consecutiveMisses;
    }

    public long getLastResponseMillis(int workerId) {
        Observation observation = observations.get(workerId);

        return observation == null
                ? -1
                : observation.lastRoundTripMillis;
    }

    public long getLastResponseAgeMillis(int workerId) {
        Observation observation = observations.get(workerId);

        if (observation == null) {
            return -1;
        }

        return (System.nanoTime() - observation.lastResponseNanos)
                / 1_000_000L;
    }

    public void removeWorker(int workerId) {
        observations.remove(workerId);
    }
}
