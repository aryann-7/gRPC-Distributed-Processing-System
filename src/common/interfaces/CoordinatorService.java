package common.interfaces;

import common.models.CoordinatorMessage;
import common.models.JobRequest;
import common.models.JobResult;

/**
 * Coordinator interface for cluster job management and delegation.
 */
public interface CoordinatorService {

    /**
     * Submits a batch computation job from the client.
     *
     * @param request The job request parameters submitted by the client.
     * @return JobResult containing computed values or status.
     */
    JobResult submitJob(JobRequest request);

    /**
     * Receives heartbeat or coordinator management messages from cluster nodes.
     *
     * @param message Coordinator message payload.
     */
    void handleCoordinatorMessage(CoordinatorMessage message);

    /**
     * Queries the current status of the coordinator including the Job Allocation Counter (JAC) and term.
     *
     * @return String or status representation.
     */
    String getCoordinatorStatus();
}

