package client;

import client.gui.ClientGuiFrame;
import javax.swing.SwingUtilities;
import javax.swing.UIManager;

// GUI launch point.

public class ClientMain {

    public static void main(String[] args) {
        String clientId = (args.length > 0 && !args[0].trim().isEmpty())
                ? args[0].trim()
                : "1";

        try {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        } catch (Exception ignored) {
        }

        SwingUtilities.invokeLater(() -> {
            ClientGuiFrame frame = new ClientGuiFrame(clientId);
            frame.setVisible(true);
        });
    }
}
