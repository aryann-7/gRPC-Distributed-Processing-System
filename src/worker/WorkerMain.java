package worker;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Starts a standalone Worker process.
 *
 * Arguments:
 *
 * workerId host workerPort bootstrapHost bootstrapPort
 *
 * Example:
 *
 * 101 localhost 1101 localhost 1099
 */
public class WorkerMain {

        public static void main(String[] args) {

                if (args.length < 5) {

                        System.out.println(
                                        "Usage:");
                        System.out.println(
                                        "WorkerMain <workerId> <host> "
                                                        + "<workerPort> <bootstrapHost> "
                                                        + "<bootstrapPort>");

                        return;
                }

                try {

                        int workerId = Integer.parseInt(args[0]);

                        String host = args[1];

                        int workerPort = Integer.parseInt(args[2]);

                        String bootstrapHost = args[3];

                        int bootstrapPort = Integer.parseInt(args[4]);

                        /*
                         * Create an RMI registry for this worker.
                         */
                        Registry registry = LocateRegistry.createRegistry(
                                        workerPort);

                        /*
                         * Create worker.
                         */
                        WorkerNode worker = new WorkerNode(
                                        workerId,
                                        host,
                                        workerPort);

                        /*
                         * Bind worker to its registry.
                         */
                        registry.rebind(
                                        "Worker-" + workerId,
                                        worker);

                        System.out.println(
                                        "================================");

                        System.out.println(
                                        "Worker " + workerId +
                                                        " started.");

                        System.out.println(
                                        "Port: " + workerPort);

                        System.out.println(
                                        "JAC: " + worker.getJac());

                        System.out.println(
                                        "================================");

                        // Register with Bootstrap.
                        worker.connectToBootstrap(
                                        bootstrapHost,
                                        bootstrapPort);

                        /*
                         * Start election after a short delay to allow cluster peers to join and form the mesh.
                         * To prevent simultaneous conflicting election trees on boot, only the primary worker
                         * (worker 101 or when specifically requested) initiates the election flood.
                         */
                        if (workerId == 101 || Boolean.getBoolean("cs324.initiateElection")) {
                                Thread.sleep(3000); // Wait 3 seconds for cluster topology to settle
                                System.out.println("Worker " + workerId + " initiating initial cluster election...");
                                worker.startElection();
                        }

                        /*
                         * Keep the process alive.
                         */
                        synchronized (worker) {

                                worker.wait();
                        }

                } catch (Exception e) {

                        e.printStackTrace();
                }
        }
}