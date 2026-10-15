package bootstrap;

import common.grpc.WorkerDescriptor;
import common.grpc.WorkerEndpoint;
import common.grpc.WorkerIdentity;
import common.grpc.MembershipSnapshot;
import common.grpc.CoordinatorRecord;
import common.grpc.MembershipChange;
import common.grpc.MembershipUpdate;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

public final class WorkerRegistry {
    private static final int MAX_PENDING_UPDATES = 256;
    // One lock keeps membership checks, changes and snapshots consistent.
    private final Map<Integer, Registration> workers = new HashMap<>();
    private final Set<MembershipWatch> watches = new HashSet<>();
    private final int maxNeighbours;
    private final LongSupplier nanoTime;
    private final String epoch = UUID.randomUUID().toString();
    private long revision;
    private CoordinatorRecord coordinator;
    private int highestCoordinatorTerm;

    public WorkerRegistry() {
        this(Integer.getInteger("cs324.maxNeighbors", 2));
    }

    public WorkerRegistry(int maxNeighbours) {
        this(maxNeighbours, System::nanoTime);
    }

    WorkerRegistry(int maxNeighbours, LongSupplier nanoTime) {
        this.maxNeighbours = Math.max(1, maxNeighbours);
        this.nanoTime = Objects.requireNonNull(nanoTime);
    }

    private static final class Registration {
        private final WorkerDescriptor worker;
        private long leaseDurationNanos;
        private long leaseDeadlineNanos;

        private Registration(WorkerDescriptor worker) {
            this.worker = worker;
        }
    }

    // True for a new worker, false for an exact retry.
    public synchronized boolean registerWorker(WorkerDescriptor worker) {
        validateWorker(worker);
        int id = worker.getIdentity().getWorkerId();
        Registration existing = workers.get(id);
        if (existing != null) {
            if (!existing.worker.equals(worker)) {
                throw new IllegalStateException("Worker ID " + id + " is already registered");
            }
            if (existing.leaseDurationNanos != 0) {
                long now = nanoTime.getAsLong();
                requireLiveLease(existing, now);
                existing.leaseDeadlineNanos = now + existing.leaseDurationNanos;
            }
            return false;
        }
        workers.put(id, new Registration(worker));
        membershipChanged(MembershipChange.Kind.JOINED, worker);
        return true;
    }

    public synchronized Optional<WorkerDescriptor> findWorker(int workerId) {
        validateWorkerId(workerId);
        return Optional.ofNullable(workers.get(workerId)).map(registration -> registration.worker);
    }

    public synchronized List<WorkerDescriptor> snapshot() {
        return workers.values().stream().map(registration -> registration.worker).toList();
    }

    // First renewal opts this session into leases; registration alone remains permanent.
    public synchronized void renewLease(WorkerIdentity identity, long durationMillis) {
        validateIdentity(identity);
        if (durationMillis <= 0 || durationMillis > Long.MAX_VALUE / 1_000_000) {
            throw new IllegalArgumentException("Lease duration must be positive and fit in nanoseconds");
        }
        Registration registration = workers.get(identity.getWorkerId());
        if (registration == null) {
            throw new NoSuchElementException("Worker ID " + identity.getWorkerId() + " is not registered");
        }
        requireSession(identity, registration.worker);
        long now = nanoTime.getAsLong();
        requireLiveLease(registration, now);
        registration.leaseDurationNanos = durationMillis * 1_000_000;
        registration.leaseDeadlineNanos = now + registration.leaseDurationNanos;
    }

    public synchronized int expireLeases() {
        long now = nanoTime.getAsLong();
        List<WorkerIdentity> expired = workers.values().stream()
                .filter(registration -> leaseExpired(registration, now))
                .map(registration -> registration.worker.getIdentity()).toList();
        expired.forEach(this::deregisterWorker);
        return expired.size();
    }

    private static boolean leaseExpired(Registration registration, long now) {
        // Subtraction keeps nanoTime deadlines valid across signed counter wraparound.
        return registration.leaseDurationNanos != 0 && now - registration.leaseDeadlineNanos >= 0;
    }

    private static void requireLiveLease(Registration registration, long now) {
        if (leaseExpired(registration, now)) {
            throw new IllegalStateException("Worker lease has expired");
        }
    }

    public String getEpoch() {
        return epoch;
    }

    public synchronized MembershipSnapshot membershipSnapshot() {
        return MembershipSnapshot.newBuilder().setRegistryEpoch(epoch)
                .setMembershipRevision(revision).addAllWorkers(snapshot()).build();
    }

    public synchronized MembershipWatch watchMembership() {
        MembershipWatch watch = new MembershipWatch();
        // Capture and subscribe under the same lock so no join can fall between them.
        watch.pending.add(MembershipUpdate.newBuilder().setSnapshot(membershipSnapshot()).build());
        watches.add(watch);
        return watch;
    }

    synchronized int membershipWatchCount() {
        return watches.size();
    }

    private void membershipChanged(MembershipChange.Kind kind, WorkerDescriptor worker) {
        MembershipUpdate update = MembershipUpdate.newBuilder().setChange(MembershipChange.newBuilder()
                .setKind(kind).setWorker(worker).setRegistryEpoch(epoch).setMembershipRevision(++revision)).build();
        var iterator = watches.iterator();
        while (iterator.hasNext()) {
            MembershipWatch watch = iterator.next();
            if (watch.pending.size() == MAX_PENDING_UPDATES) {
                watch.overflowed = true;
                watch.closed = true;
                watch.pending.clear();
                iterator.remove();
            } else {
                watch.pending.add(update);
            }
        }
        notifyAll();
    }

    public final class MembershipWatch implements AutoCloseable {
        private final ArrayDeque<MembershipUpdate> pending = new ArrayDeque<>();
        private boolean closed;
        private boolean overflowed;
        private boolean paused;

        private MembershipWatch() {}

        // Empty means closed; overflow requires a fresh snapshot and subscription.
        public Optional<MembershipUpdate> nextUpdate() throws InterruptedException {
            synchronized (WorkerRegistry.this) {
                while ((pending.isEmpty() || paused) && !closed) {
                    WorkerRegistry.this.wait();
                }
                if (overflowed) {
                    throw new IllegalStateException("Membership watch overflowed; subscribe again");
                }
                return Optional.ofNullable(pending.poll());
            }
        }

        public void setPaused(boolean paused) {
            synchronized (WorkerRegistry.this) {
                this.paused = paused;
                WorkerRegistry.this.notifyAll();
            }
        }

        @Override
        public void close() {
            synchronized (WorkerRegistry.this) {
                watches.remove(this);
                closed = true;
                pending.clear();
                WorkerRegistry.this.notifyAll();
            }
        }
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
        Registration registered = workers.get(record.getCoordinator().getIdentity().getWorkerId());
        if (registered == null || !record.getCoordinator().equals(registered.worker)) {
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
        Registration existing = workers.get(identity.getWorkerId());
        if (existing == null) {
            return false;
        }
        requireSession(identity, existing.worker);
        workers.remove(identity.getWorkerId());
        if (coordinator != null && coordinator.getCoordinator().getIdentity().getWorkerId() == identity.getWorkerId()) {
            coordinator = null;
        }
        membershipChanged(MembershipChange.Kind.LEFT, existing.worker);
        return true;
    }

    public synchronized List<WorkerDescriptor> getInitialNeighbours(WorkerIdentity requester) {
        validateIdentity(requester);
        Registration existing = workers.get(requester.getWorkerId());
        if (existing == null) {
            throw new NoSuchElementException("Worker ID " + requester.getWorkerId() + " is not registered");
        }
        requireSession(requester, existing.worker);
        List<WorkerDescriptor> candidates = new ArrayList<>();
        for (Registration registration : workers.values()) {
            WorkerDescriptor worker = registration.worker;
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
