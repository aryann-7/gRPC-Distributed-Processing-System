package client;

import client.api.CoordinatorClientFactory;
import client.api.RetryingCoordinatorClient;
import client.grpc.ChannelFactory;
import client.grpc.GrpcCoordinatorClient;
import client.gui.ClientGuiFrame;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

/**
 * Starts one client process.
 *
 *   java client.ClientMain [clientId] [bootstrapHost] [bootstrapPort]
 *   defaults:               1          localhost       50050
 *
 * Run several at once (different IDs) to have multiple clients.
 */
public class ClientMain {

    /** Attempts per job, including the first. */
    private static final int MAX_ATTEMPTS = 5;
    /** Pause before resubmitting, giving the cluster time to elect a new coordinator. */
    private static final long RETRY_DELAY_MS = 2000;

    public static void main(String[] args) {
        String clientId = argOrDefault(args, 0, "1");
        String bootstrapHost = argOrDefault(args, 1, "localhost");
        int bootstrapPort = Integer.parseInt(argOrDefault(args, 2, "50050"));

        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
            // Cosmetic only; the default look and feel is fine.
        }

        CoordinatorClientFactory factory = (host, port) -> new RetryingCoordinatorClient(
                new GrpcCoordinatorClient(clientId, host, port, ChannelFactory.plaintext()),
                MAX_ATTEMPTS, RETRY_DELAY_MS);

        SwingUtilities.invokeLater(() ->
                new ClientGuiFrame(clientId, bootstrapHost, bootstrapPort, factory).setVisible(true));
    }

    private static String argOrDefault(String[] args, int index, String fallback) {
        return (args.length > index && !args[index].trim().isEmpty()) ? args[index].trim() : fallback;
    }
}
