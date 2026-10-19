package worker;

/**
 * Starts a standalone Worker process using pure gRPC.
 *
 * Arguments:
 *
 * workerId host workerPort bootstrapHost bootstrapPort [grpcPort]
 *
 * Example:
 *
 * 101 localhost 1101 localhost 50050 50051
 */
public class WorkerMain {

        public static void main(String[] args) {

                if (args.length < 5 || args.length > 6) {

                        System.out.println(
                                        "Usage:");
                        System.out.println(
                                        "WorkerMain <workerId> <host> "
                                                        + "<workerPort> <bootstrapHost> "
                                                        + "<bootstrapPort> [grpcPort; default 0 = assigned by OS]");

                        return;
                }

                WorkerNode worker = null;
                Thread shutdownHook = null;
                boolean interrupted = false;
                try {

                        int workerId = Integer.parseInt(args[0]);
                        String host = args[1];
                        int workerPort = Integer.parseInt(args[2]);
                        String bootstrapHost = args[3];
                        int bootstrapPort = Integer.parseInt(args[4]);
                        int grpcPort = args.length == 6 ? Integer.parseInt(args[5]) : 0;

                        /*
                         * Create worker with pure gRPC server.
                         */
                        worker = new WorkerNode(
                                        workerId,
                                        host,
                                        workerPort, grpcPort);

                        WorkerNode runningWorker = worker;
                        shutdownHook = new Thread(() -> shutdown(runningWorker), "worker-shutdown");
                        Runtime.getRuntime().addShutdownHook(shutdownHook);

                        System.out.println(
                                        "================================");

                        System.out.println(
                                        "Worker " + workerId +
                                                        " started.");

                        System.out.println(
                                        "Port: " + workerPort + "; gRPC Port: " + worker.getGrpcServer().getPort());

                        System.out.println(
                                        "JAC: " + worker.getJac());

                        System.out.println(
                                        "================================");

                        // Register with Bootstrap via gRPC.
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

                } catch (InterruptedException e) {
                        interrupted = true;
                } catch (Exception e) {
                        throw new IllegalStateException("Worker startup failed", e);
                } finally {
                        if (shutdownHook != null) {
                                try {
                                        Runtime.getRuntime().removeShutdownHook(shutdownHook);
                                } catch (IllegalStateException ignored) {
                                        // The JVM is already running shutdown hooks.
                                }
                        }
                        shutdown(worker);
                        if (interrupted) {
                                Thread.currentThread().interrupt();
                        }
                }
        }

        private static void shutdown(WorkerNode worker) {
                if (worker != null) {
                        worker.stop();
                }
        }
}

