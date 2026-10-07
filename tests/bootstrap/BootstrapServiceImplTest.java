package bootstrap;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BootstrapServiceImplTest {

    public static void main(String[] args) throws RemoteException {
        testRegistration();
        System.out.println("PASS: worker registration");
        testPeerDiscovery();
        System.out.println("PASS: initial peer discovery");
        testDeregistration();
        System.out.println("PASS: worker deregistration");
        System.out.println("All Bootstrap registry tests passed.");
    }

    private static void testRegistration() throws RemoteException {
        BootstrapServiceImpl service = new BootstrapServiceImpl();
        try {
            service.registerWorker(1, "localhost", 1);
            service.registerWorker(Integer.MAX_VALUE, "localhost", 65535);
            check(service.getInitialNeighbors(1).equals(Collections.singletonList(Integer.MAX_VALUE + "|localhost|65535")),
                    "Maximum positive worker ID and port should be accepted");
            check(service.getInitialNeighbors(Integer.MAX_VALUE).equals(Collections.singletonList("1|localhost|1")),
                    "Worker ID 1 and port 1 should be accepted");

            expectRegistrationRejected(service, 1, "localhost", 1);
            expectRegistrationRejected(service, 1, "other-host", 1102);
            check(service.getInitialNeighbors(Integer.MAX_VALUE).equals(Collections.singletonList("1|localhost|1")),
                    "Duplicate registration must not replace the original endpoint");

            expectRegistrationRejected(service, 101, null, 1101);
            expectRegistrationRejected(service, 101, "", 1101);
            expectRegistrationRejected(service, 101, " \t ", 1101);
            expectRegistrationRejected(service, 101, "localhost", -1);
            expectRegistrationRejected(service, 101, "localhost", 0);
            expectRegistrationRejected(service, 101, "localhost", 65536);
            expectRegistrationRejected(service, 0, "localhost", 1101);
            expectRegistrationRejected(service, -1, "localhost", 1101);
            expectRegistrationRejected(service, Integer.MIN_VALUE, "localhost", 1101);

            service.registerWorker(101, "localhost", 1101);
            check(service.getInitialNeighbors(101).size() == 2,
                    "Failed registration attempts must not prevent a later valid registration");
        } finally {
            UnicastRemoteObject.unexportObject(service, true);
        }
    }

    private static void testPeerDiscovery() throws RemoteException {
        BootstrapServiceImpl service = new BootstrapServiceImpl();
        try {
            expectDiscoveryRejected(service, 999);
            service.registerWorker(101, "localhost", 1101);
            check(service.getInitialNeighbors(101).isEmpty(),
                    "The first worker should have no neighbors");

            service.registerWorker(102, "localhost", 1102);
            check(service.getInitialNeighbors(102).equals(Collections.singletonList("101|localhost|1101")),
                    "The second worker should receive the first worker");

            service.registerWorker(103, "localhost", 1103);
            service.registerWorker(104, "localhost", 1104);
            List<String> endpoints = Arrays.asList(
                    "101|localhost|1101", "102|localhost|1102", "103|localhost|1103", "104|localhost|1104");
            for (int workerId = 101; workerId <= 104; workerId++) {
                String ownEndpoint = workerId + "|localhost|" + (workerId + 1000);
                for (int attempt = 0; attempt < 100; attempt++) {
                    List<String> neighbors = service.getInitialNeighbors(workerId);
                    check(neighbors.size() == 2, "Discovery should return up to 2 distinct peers for resilience");
                    check(!neighbors.get(0).equals(neighbors.get(1)), "Returned peers must be distinct");
                    check(endpoints.contains(neighbors.get(0)) && endpoints.contains(neighbors.get(1)), "Peers must be registered workers");
                    check(!ownEndpoint.equals(neighbors.get(0)) && !ownEndpoint.equals(neighbors.get(1)), "A worker must not receive itself");
                }
            }
            expectDiscoveryRejected(service, 999);
        } finally {
            UnicastRemoteObject.unexportObject(service, true);
        }
    }

    private static void testDeregistration() throws RemoteException {
        BootstrapServiceImpl service = new BootstrapServiceImpl();
        try {
            service.deregisterWorker(999);
            service.registerWorker(101, "localhost", 1101);
            service.registerWorker(102, "localhost", 1102);
            service.registerWorker(103, "localhost", 1103);

            service.deregisterWorker(102);
            check(service.getInitialNeighbors(101).equals(Collections.singletonList("103|localhost|1103")),
                    "A removed worker must not be eligible for discovery");
            check(service.getInitialNeighbors(103).equals(Collections.singletonList("101|localhost|1101")),
                    "Removal must preserve the other registered workers");
            expectDiscoveryRejected(service, 102);

            service.deregisterWorker(999);
            service.deregisterWorker(102);
            check(service.getInitialNeighbors(101).equals(Collections.singletonList("103|localhost|1103")),
                    "Unknown or repeated deregistration must preserve the registry");
            check(service.getInitialNeighbors(103).equals(Collections.singletonList("101|localhost|1101")),
                    "Unknown or repeated deregistration must preserve the registry");
            expectDiscoveryRejected(service, 102);

            service.deregisterWorker(103);
            check(service.getInitialNeighbors(101).isEmpty(),
                    "The last remaining worker should have no neighbors");
            service.deregisterWorker(101);
            expectDiscoveryRejected(service, 101);
        } finally {
            UnicastRemoteObject.unexportObject(service, true);
        }
    }

    private static void expectRegistrationRejected(BootstrapServiceImpl service, int workerId,
            String host, int port) throws RemoteException {
        try {
            service.registerWorker(workerId, host, port);
        } catch (RemoteException expected) {
            return;
        }
        throw new AssertionError("Expected registration to reject worker " + workerId
                + " at " + host + ":" + port);
    }

    private static void expectDiscoveryRejected(BootstrapServiceImpl service, int workerId)
            throws RemoteException {
        try {
            service.getInitialNeighbors(workerId);
        } catch (RemoteException expected) {
            return;
        }
        throw new AssertionError("Expected discovery to reject unregistered worker " + workerId);
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
