package common.interfaces;

import common.models.CoordinatorMessage;
import common.models.JobRequest;
import common.models.JobResult;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for the active Coordinator node in the cluster.
 * Handles client job submissions, job delegation to workers, and coordinator heartbeat/announcements.
 */
public interface CoordinatorService extends Remote {

    /**
     * Submits a batch computation job from the client.
     *
     * @param request The job request parameters submitted by the client.
     * @return JobResult containing computed values or status.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    JobResult submitJob(JobRequest request) throws RemoteException;

    /**
     * Receives heartbeat or coordinator management messages from cluster nodes.
     *
     * @param message Coordinator message payload.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    void handleCoordinatorMessage(CoordinatorMessage message) throws RemoteException;

    /**
     * Queries the current status of the coordinator including the Job Allocation Counter (JAC) and term.
     *
     * @return String or status representation.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    String getCoordinatorStatus() throws RemoteException;
}
