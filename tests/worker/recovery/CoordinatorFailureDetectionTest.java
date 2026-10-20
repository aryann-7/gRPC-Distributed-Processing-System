package worker.recovery;

import bootstrap.BootstrapGrpcServer;
import bootstrap.WorkerRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import worker.WorkerNode;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class CoordinatorFailureDetectionTest {
    private final List<WorkerNode> workers = new ArrayList<>();
    private BootstrapGrpcServer bootstrapServer;

    @AfterEach
    public void tearDown() throws Exception {
        workers.forEach(WorkerNode::stop);
        if (bootstrapServer != null) {
            bootstrapServer.close();
        }
    }

    @Test
    public void workerDetectsDeadCoordinatorAndElectsNewLeader() throws Exception {
        WorkerRegistry registry = new WorkerRegistry(2);
        bootstrapServer = new BootstrapGrpcServer(0, registry);
        bootstrapServer.start();
        int bsPort = bootstrapServer.getPort();

        int p1, p2;
        try (ServerSocket s1 = new ServerSocket(0); ServerSocket s2 = new ServerSocket(0)) {
            p1 = s1.getLocalPort();
            p2 = s2.getLocalPort();
        }

        WorkerNode worker1 = new WorkerNode(1, "localhost", p1);
        workers.add(worker1);
        worker1.connectToBootstrap("localhost", bsPort);

        WorkerNode worker2 = new WorkerNode(2, "localhost", p2);
        workers.add(worker2);
        worker2.connectToBootstrap("localhost", bsPort);

        // Make worker 1 the coordinator
        worker1.transitionToCoordinator();
        assertTrue(worker1.isCoordinator());

        // Refresh worker 2 so it sees coordinator
        worker2.refreshNeighboursFromBootstrap();
        try (worker.grpc.BootstrapGrpcClient bsClient = new worker.grpc.BootstrapGrpcClient("localhost", bsPort)) {
            var coordResp = bsClient.getCoordinator();
            assertTrue(coordResp.hasCoordinator());
            assertEquals(1, coordResp.getCoordinator().getCoordinator().getIdentity().getWorkerId());
        }

        // Set worker 2's coordinator tracking
        common.models.ElectionMessage coordMsg = new common.models.ElectionMessage(
                "msg-init",
                common.models.ElectionMessage.MessageType.COORDINATOR,
                1, 0, 1, 1);
        worker2.receiveElectionMessage(coordMsg);
        assertEquals(1, worker2.getCoordinatorId());
        assertFalse(worker2.isCoordinator());

        // Now kill worker 1
        worker1.stop();

        // Worker 2's health checker checks every 2000ms and detects after 2 missed pings
        long deadline = System.currentTimeMillis() + 10_000;
        boolean elected = false;
        while (System.currentTimeMillis() < deadline) {
            if (worker2.isCoordinator() && worker2.getCoordinatorId() == 2) {
                elected = true;
                break;
            }
            Thread.sleep(200);
        }

        assertTrue(elected, "Worker 2 should have autonomously detected Coordinator 1 failure and elected itself coordinator");

        // Verify Bootstrap has worker 2 as coordinator
        try (worker.grpc.BootstrapGrpcClient bsClient = new worker.grpc.BootstrapGrpcClient("localhost", bsPort)) {
            var coordResp = bsClient.getCoordinator();
            assertTrue(coordResp.hasCoordinator());
            assertEquals(2, coordResp.getCoordinator().getCoordinator().getIdentity().getWorkerId());
        }
    }
}
