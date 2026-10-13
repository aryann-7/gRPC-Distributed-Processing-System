package client.api;

import client.model.JobSpec;

/**
 * How the client talks to the cluster's coordinator.
 *
 * The GUI only uses this interface, so swapping RMI for gRPC means adding
 * one new implementation; no GUI code changes. Implementations must:
 * - return from submit() immediately (never block the caller), and
 * - report the outcome through the listener on their own threads.
 */
public interface CoordinatorClient extends AutoCloseable {

    /** Starts the job asynchronously and reports progress/outcome to listener. */
    void submit(JobSpec job, JobProgressListener listener);

    /**
     * Blocking check that a coordinator can currently be reached.
     *
     * @return a short description of what was found
     * @throws Exception describing why it could not be reached
     */
    String checkConnection() throws Exception;

    /** Releases threads and connections. */
    @Override
    void close();
}
