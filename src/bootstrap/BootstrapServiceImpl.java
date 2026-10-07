package bootstrap;

import common.interfaces.BootstrapService;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Bootstrap Node.
 *
 * The Bootstrap Node only helps workers join the network.
 * It does not participate in elections or computation.
 */
public class BootstrapServiceImpl
        extends UnicastRemoteObject
        implements BootstrapService {

    private static final long serialVersionUID = 1L;

    /**
     * Worker ID -> endpoint ("workerId|host|port").
     */
    private final Map<Integer, String> registeredWorkers =
            new ConcurrentHashMap<>();

    private final int maxNeighborsPerWorker;

    public BootstrapServiceImpl()
            throws RemoteException {

        this(Integer.getInteger("cs324.maxNeighbors", 2));
    }

    public BootstrapServiceImpl(int maxNeighborsPerWorker)
            throws RemoteException {

        super();
        this.maxNeighborsPerWorker = Math.max(1, maxNeighborsPerWorker);
    }

    /**
     * Registers an active worker.
     */
    @Override
    public void registerWorker(
            int workerId,
            String host,
            int port)
            throws RemoteException {

        if (workerId <= 0) {
            throw new RemoteException("Worker ID must be greater than 0: " + workerId);
        }

        if (host == null || host.trim().isEmpty()) {
            throw new RemoteException("Worker host cannot be empty");
        }

        if (port <= 0 || port > 65535) {
            throw new RemoteException("Invalid worker port: " + port);
        }

        String endpoint = workerId + "|" + host + "|" + port;

        String existing = registeredWorkers.putIfAbsent(workerId, endpoint);
        if (existing != null) {
            throw new RemoteException("Worker ID " + workerId + " is already registered");
        }

        System.out.println(
                "Registered Worker " +
                workerId +
                " at " +
                host +
                ":" +
                port);
    }

    /**
     * Gives a joining worker one random active neighbour.
     */
    @Override
    public List<String> getInitialNeighbors(
            int workerId)
            throws RemoteException {

        if (!registeredWorkers.containsKey(workerId)) {
            throw new RemoteException("Worker ID " + workerId + " is not registered");
        }

        List<String> candidates = new ArrayList<>();
        for (Map.Entry<Integer, String> entry : registeredWorkers.entrySet()) {
            if (entry.getKey() != workerId) {
                candidates.add(entry.getValue());
            }
        }

        if (candidates.isEmpty()) {
            return Collections.emptyList();
        }

        // Select up to maxNeighborsPerWorker random distinct neighbors (min(maxNeighborsPerWorker, candidates.size()))
        // to form a resilient connected mesh rather than a fragile single linear tree.
        Collections.shuffle(candidates, ThreadLocalRandom.current());
        int count = Math.min(maxNeighborsPerWorker, candidates.size());
        List<String> selected = new ArrayList<>(candidates.subList(0, count));

        System.out.println(
                "Worker " + workerId +
                " assigned initial neighbours: " +
                selected);

        return selected;
    }

    /**
     * Removes a worker from the active registry.
     */
    @Override
    public void deregisterWorker(
            int workerId)
            throws RemoteException {

        String removed = registeredWorkers.remove(workerId);
        if (removed != null) {
            System.out.println(
                    "Worker " + workerId +
                    " deregistered from " +
                    removed);
        } else {
            System.out.println(
                    "Worker " + workerId +
                    " is not registered");
        }
    }
}