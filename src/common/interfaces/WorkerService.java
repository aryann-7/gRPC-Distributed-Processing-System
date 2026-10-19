package common.interfaces;

import common.models.ElectionMessage;
import common.models.ElectionReply;
import common.models.JobResult;
import common.models.JobTask;

/**
 * Cluster worker interface for internal dispatch, election routing, and health checks.
 * All distributed communications are backed by pure gRPC.
 */
public interface WorkerService {

    /**
     * Stores a neighbour without calling back or contacting Bootstrap.
     * Repeated registration of the same ID preserves the existing neighbour.
     *
     * @param neighbourId positive worker ID, different from this worker's ID
     * @param neighbour reference to the neighbour
     */
    void addNeighbour(int neighbourId, WorkerService neighbour);

    /**
     * Executes a distributed computation task.
     */
    JobResult executeJob(JobTask task);

    /**
     * Receives and propagates ELECTION and COORDINATOR messages.
     */
    void receiveElectionMessage(ElectionMessage message);

    /**
     * Sends the best election candidate back toward the
     * worker that started the election.
     */
    void receiveElectionReply(ElectionReply reply);

    /**
     * Checks whether this worker is alive.
     */
    boolean ping();
}

