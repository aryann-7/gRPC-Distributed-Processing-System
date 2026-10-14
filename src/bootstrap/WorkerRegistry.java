package bootstrap;

import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import common.grpc.MembershipSnapshot;
import common.grpc.CoordinatorRecord;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class WorkerRegistry {
    // One lock keeps membership checks, changes and snapshots consistent.
    private final Map<Integer, WorkerDescriptor> workers = new HashMap<>();
    private final int maxNeighbours;
    private final String epoch = UUID.randomUUID().toString();
    private long revision;
    private CoordinatorRecord coordinator;
    private int highestCoordinatorTerm;

    public WorkerRegistry() {
        this(Integer.getInteger("cs324.maxNeighbors", 2));
    }

    public WorkerRegistry(int maxNeighbours) {
        this.maxNeighbours = Math.max(1, maxNeighbours);
    }

    // True for a new worker, false for an exact retry.
    public synchronized boolean registerWorker(WorkerDescriptor worker) {
        validateWorker(worker);
        int id = worker.getIdentity().getWorkerId();
        WorkerDescriptor existing = workers.get(id);
        if (existing != null) {
            if (!existing.equals(worker)) {
                throw new IllegalStateException("Worker ID " + id + " is already registered");
            }
            return false;
        }
        workers.put(id, worker);
        revision++;
        return true;
    }

    public synchronized Optional<WorkerDescriptor> findWorker(int workerId) {
        validateWorkerId(workerId);
        return Optional.ofNullable(workers.get(workerId));
    }

    public synchronized List<WorkerDescriptor> snapshot() {
        return List.copyOf(workers.values());
    }

    public String getEpoch() {
        return epoch;
    }

    public synchronized MembershipSnapshot membershipSnapshot() {
        return MembershipSnapshot.newBuilder().setRegistryEpoch(epoch)
                .setMembershipRevision(revision).addAllWorkers(workers.values()).build();
    }

    public synchronized Optional<CoordinatorRecord> getCoordinator() {
        return Optional.ofNullable(coordinator);
    }

    public synchronized boolean publishCoordinator(CoordinatorRecord record, String registryEpoch) {
        if (record == null || !record.hasCoordinator()) {
            throw new IllegalArgumentException("Coordinator descriptor is required");
        }
        validateWorker(record.getCoordinator());
        validateTerm(record.getTerm());
        requireEpoch(registryEpoch);
        WorkerDescriptor registered = workers.get(record.getCoordinator().getIdentity().getWorkerId());
        if (!record.getCoordinator().equals(registered)) {
            throw new IllegalStateException("Coordinator must match a registered worker session and endpoint");
        }
        if (record.equals(coordinator)) {
            return false;
        }
        if (record.getTerm() <= highestCoordinatorTerm) {
            throw new IllegalStateException("Coordinator term must be newer than the last publication");
        }
        coordinator = record;
        highestCoordinatorTerm = record.getTerm();
        return true;
    }

    public synchronized boolean clearCoordinator(WorkerIdentity identity, int term, String registryEpoch) {
        validateIdentity(identity);
        validateTerm(term);
        requireEpoch(registryEpoch);
        if (coordinator == null) {
            return false;
        }
        if (!coordinator.getCoordinator().getIdentity().equals(identity) || coordinator.getTerm() != term) {
            throw new IllegalStateException("Coordinator identity and term do not match");
        }
        // Keep the term watermark so a delayed publication cannot restore it.
        coordinator = null;
        return true;
    }

    private void requireEpoch(String registryEpoch) {
        if (!epoch.equals(registryEpoch)) {
            throw new IllegalStateException("Registry epoch does not match");
        }
    }

    private static void validateTerm(int term) {
        if (term <= 0) {
            throw new IllegalArgumentException("Coordinator term must be greater than zero");
        }
    }

    public synchronized boolean deregisterWorker(WorkerIdentity identity) {
        validateIdentity(identity);
        WorkerDescriptor existing = workers.get(identity.getWorkerId());
        if (existing == null) {
            return false;
        }
        requireSession(identity, existing);
        workers.remove(identity.getWorkerId());
        if (coordinator != null && coordinator.getCoordinator().getIdentity().getWorkerId() == identity.getWorkerId()) {
            coordinator = null;
        }
        revision++;
        return true;
    }

    public synchronized List<WorkerDescriptor> getInitialNeighbours(WorkerIdentity requester) {
        validateIdentity(requester);
        WorkerDescriptor existing = workers.get(requester.getWorkerId());
        if (existing == null) {
            throw new NoSuchElementException("Worker ID " + requester.getWorkerId() + " is not registered");
        }
        requireSession(requester, existing);
        List<WorkerDescriptor> candidates = new ArrayList<>();
        for (WorkerDescriptor worker : workers.values()) {
            if (worker.getIdentity().getWorkerId() != requester.getWorkerId()) {
                candidates.add(worker);
            }
        }
        Collections.shuffle(candidates, ThreadLocalRandom.current());
        return List.copyOf(candidates.subList(0, Math.min(maxNeighbours, candidates.size())));
    }

    private static void validateWorker(WorkerDescriptor worker) {
        if (worker == null || !worker.hasIdentity() || !worker.hasEndpoint()) {
            throw new IllegalArgumentException("Worker identity and endpoint are required");
        }
        validateIdentity(worker.getIdentity());
        WorkerEndpoint endpoint = worker.getEndpoint();
        String host = endpoint.getHost();
        if (endpoint.getPort() < 1 || endpoint.getPort() > 65535) {
            throw new IllegalArgumentException("Worker port must be between 1 and 65535");
        }
        if (host.isBlank() || host.contains("[") || host.contains("]")
                || host.equals("0.0.0.0") || host.matches("[0:]+")) {
            throw new IllegalArgumentException("Worker host must be an advertised hostname or IP address");
        }
        try {
            // Validate host syntax without DNS lookup or a network call.
            URI address = new URI("grpc", null, host, endpoint.getPort(), null, null, null)
                    .parseServerAuthority();
            if (!host.equals(address.getHost()) && !("[" + host + "]").equals(address.getHost())) {
                throw new IllegalArgumentException("Worker host must not contain URI components");
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid worker host: " + host, e);
        }
    }

    private static void validateIdentity(WorkerIdentity identity) {
        if (identity == null || identity.getSessionId().isBlank()) {
            throw new IllegalArgumentException("Worker session ID is required");
        }
        validateWorkerId(identity.getWorkerId());
    }

    private static void validateWorkerId(int workerId) {
        if (workerId <= 0) {
            throw new IllegalArgumentException("Worker ID must be greater than zero");
        }
    }

    private static void requireSession(WorkerIdentity requested, WorkerDescriptor existing) {
        if (!requested.getSessionId().equals(existing.getIdentity().getSessionId())) {
            throw new IllegalStateException("Worker session does not match the registered session");
        }
    }
}
