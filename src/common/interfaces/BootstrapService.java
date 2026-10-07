package common.interfaces;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

//Remote interface for the Bootstrap Node.
//Tracks active worker registries and connects joining workers randomly to an active worker.
//Must NOT participate in election or compute jobs.

public interface BootstrapService extends Remote {

    /**
     * Registers a new worker node with the bootstrap service.
     *
     * @param workerId Unique integer identifier of the worker node.
     * @param host     Host address of the worker.
     * @param port     Port on which the worker RMI registry/service is listening.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    void registerWorker(int workerId, String host, int port) throws RemoteException;

    /**
     * Retrieves an assigned active worker neighbor connection for network topology
     * formation.
     * Connects joining worker randomly to an active worker.
     *
     * @param workerId Unique integer identifier of the requesting worker.
     * @return One randomly selected active worker descriptor in workerId|host|port
     *         format (e.g., 101|localhost|1101), excluding the requesting worker;
     *         an empty list if no other workers are registered.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    List<String> getInitialNeighbors(int workerId) throws RemoteException;

    /**
     * Deregisters or removes a worker node upon shutdown or failure.
     *
     * @param workerId Unique integer identifier of the worker node.
     * @throws RemoteException if an RMI communication failure occurs.
     */
    void deregisterWorker(int workerId) throws RemoteException;
}
