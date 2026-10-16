package client.gui;

import client.api.CoordinatorClient;
import client.api.CoordinatorClientFactory;
import client.api.JobProgressListener;
import client.model.JobInputParser;
import client.model.JobKind;
import client.model.JobSpec;
import client.parser.CsvDataParser;
import client.progress.JobProgressTracker;
import client.progress.ProgressSnapshot;
import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

/**
 * Client window (Member 4): build a job (typed numbers, CSV file, or PRIMESUM range),
 * submit it without blocking, and watch every job's progress live.
 *
 * Threading rules:
 * - Swing components are only touched on the Event Dispatch Thread (EDT).
 * - All network work happens inside the CoordinatorClient's own threads.
 * - Listener callbacks arrive on those threads and hand UI work back to the
 *   EDT (JobProgressPanel.refresh and logMessage both use invokeLater).
 */
public class ClientGuiFrame extends JFrame {

    private static final long serialVersionUID = 1L;

    private final String clientId;
    private final String defaultHost;
    private final int defaultPort;
    private final CoordinatorClientFactory clientFactory;

    // Current transport client, recreated if the user changes host/port.
    private CoordinatorClient coordinatorClient;
    private String clientAddress;

    // Only used for the blocking "Test Connection" check.
    private final ExecutorService backgroundPool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "client-gui-background");
        t.setDaemon(true);
        return t;
    });

    private JTextField hostField;
    private JTextField portField;
    private JComboBox<JobKind> jobKindBox;
    private JTextArea numbersArea;
    private JButton browseCsvButton;
    private JTextField rangeStartField;
    private JTextField rangeEndField;
    private JTextArea logArea;
    private JLabel statusLabel;
    private final JobProgressPanel jobsPanel = new JobProgressPanel();

    public ClientGuiFrame(String clientId, String defaultHost, int defaultPort,
                          CoordinatorClientFactory clientFactory) {
        super("DistriLab Client - #" + clientId);
        this.clientId = clientId;
        this.defaultHost = defaultHost;
        this.defaultPort = defaultPort;
        this.clientFactory = clientFactory;
        buildUi();
    }

    // ------------------------------------------------------------------
    // Layout
    // ------------------------------------------------------------------

    private void buildUi() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(900, 780);
        setMinimumSize(new Dimension(780, 620));
        setLocationRelativeTo(null);
        addWindowListener(new WindowAdapter() {
            @Override
            public void windowClosing(WindowEvent e) {
                shutdown();
            }
        });

        JPanel main = new JPanel(new BorderLayout(10, 10));
        main.setBorder(BorderFactory.createEmptyBorder(10, 10, 10, 10));
        main.add(buildConnectionPanel(), BorderLayout.NORTH);

        JPanel top = new JPanel(new BorderLayout(10, 10));
        top.add(buildJobForm(), BorderLayout.CENTER);

        logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        JScrollPane logScroll = new JScrollPane(logArea);
        logScroll.setBorder(BorderFactory.createTitledBorder("Execution Log"));

        JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, jobsPanel, logScroll);
        split.setResizeWeight(0.55);
        top.add(split, BorderLayout.SOUTH);
        split.setPreferredSize(new Dimension(800, 380));
        main.add(top, BorderLayout.CENTER);

        statusLabel = new JLabel(" Ready (Client #" + clientId + ")");
        statusLabel.setForeground(new Color(0, 100, 0));
        main.add(statusLabel, BorderLayout.SOUTH);

        setContentPane(main);
        updateInputMode();
    }

    private JPanel buildConnectionPanel() {
        JPanel panel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        panel.setBorder(BorderFactory.createTitledBorder("Cluster Discovery (Bootstrap gRPC address)"));
        panel.add(new JLabel("Host:"));
        hostField = new JTextField(defaultHost, 12);
        panel.add(hostField);
        panel.add(new JLabel("Port:"));
        portField = new JTextField(String.valueOf(defaultPort), 6);
        panel.add(portField);
        JButton testButton = new JButton("Test Connection");
        testButton.addActionListener(e -> testConnection());
        panel.add(testButton);
        return panel;
    }

    private JPanel buildJobForm() {
        JPanel form = new JPanel(new GridBagLayout());
        form.setBorder(BorderFactory.createTitledBorder("New Job"));
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.BOTH;
        c.insets = new Insets(4, 4, 4, 4);

        // Row 0: job type
        c.gridx = 0; c.gridy = 0; c.weightx = 0.2;
        form.add(new JLabel("Job Type:"), c);
        jobKindBox = new JComboBox<>(JobKind.values());
        jobKindBox.addActionListener(e -> updateInputMode());
        c.gridx = 1; c.weightx = 0.8;
        form.add(jobKindBox, c);

        // Row 1: number list (MAX / PRIMECOUNT)
        c.gridx = 0; c.gridy = 1; c.weightx = 0.2;
        form.add(new JLabel("Numbers (MAX / PRIMECOUNT):"), c);
        numbersArea = new JTextArea("10, 25, 3, 99, 17, 4, 101, 88, 2, 7", 4, 30);
        numbersArea.setLineWrap(true);
        numbersArea.setWrapStyleWord(true);
        c.gridx = 1; c.weightx = 0.8;
        form.add(new JScrollPane(numbersArea), c);

        // Row 2: CSV
        c.gridx = 0; c.gridy = 2; c.weightx = 0.2;
        form.add(new JLabel("Or Load CSV:"), c);
        browseCsvButton = new JButton("Browse CSV File...");
        browseCsvButton.addActionListener(e -> loadCsv());
        c.gridx = 1; c.weightx = 0.8;
        form.add(browseCsvButton, c);

        // Row 3: range (PRIMESUM)
        c.gridx = 0; c.gridy = 3; c.weightx = 0.2;
        form.add(new JLabel("Range (PRIMESUM):"), c);
        JPanel range = new JPanel(new FlowLayout(FlowLayout.LEFT, 8, 0));
        range.add(new JLabel("Start:"));
        rangeStartField = new JTextField("1", 10);
        range.add(rangeStartField);
        range.add(new JLabel("End:"));
        rangeEndField = new JTextField("1000", 10);
        range.add(rangeEndField);
        c.gridx = 1; c.weightx = 0.8;
        form.add(range, c);

        // Row 4: submit
        JButton submitButton = new JButton("Submit Job (Async)");
        submitButton.setFont(new Font("SansSerif", Font.BOLD, 13));
        submitButton.addActionListener(e -> submitJob());
        c.gridx = 0; c.gridy = 4; c.gridwidth = 2;
        form.add(submitButton, c);
        return form;
    }

    /** PRIMESUM takes a range; the other jobs take a number list. */
    private void updateInputMode() {
        boolean range = ((JobKind) jobKindBox.getSelectedItem()).isRangeBased();
        rangeStartField.setEnabled(range);
        rangeEndField.setEnabled(range);
        numbersArea.setEnabled(!range);
        browseCsvButton.setEnabled(!range);
    }

    // ------------------------------------------------------------------
    // Actions (run on the EDT)
    // ------------------------------------------------------------------

    private void loadCsv() {
        JFileChooser chooser = new JFileChooser(sampleDataFolder());
        chooser.setDialogTitle("Select CSV dataset file");
        if (chooser.showOpenDialog(this) != JFileChooser.APPROVE_OPTION) {
            return;
        }
        File file = chooser.getSelectedFile();
        try {
            List<Long> numbers = CsvDataParser.parseCsv(file);
            if (numbers.isEmpty()) {
                showError("No valid numbers found in CSV file.");
                return;
            }
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < numbers.size(); i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(numbers.get(i));
            }
            numbersArea.setText(sb.toString());
            logMessage("Loaded " + numbers.size() + " data points from " + file.getName());
        } catch (Exception e) {
            showError("Failed to read CSV: " + e.getMessage());
        }
    }

    /** Opens the file chooser in the repo's data/ folder when it can be found. */
    private static File sampleDataFolder() {
        for (File candidate : new File[] {new File("data"), new File("../data")}) {
            if (candidate.isDirectory()) {
                return candidate;
            }
        }
        return new File(".");
    }

    private void testConnection() {
        CoordinatorClient client;
        try {
            client = currentClient();
        } catch (IllegalArgumentException e) {
            showError(e.getMessage());
            return;
        }
        logMessage("Testing connection via " + clientAddress + "...");
        backgroundPool.submit(() -> {
            try {
                String found = client.checkConnection();
                logMessage("SUCCESS: " + found);
                setStatus("Coordinator reachable");
            } catch (Exception e) {
                logMessage("ERROR: Could not reach a coordinator: " + e.getMessage());
                setStatus("Coordinator not found");
            }
        });
    }

    private void submitJob() {
        JobSpec job;
        CoordinatorClient client;
        try {
            job = JobInputParser.build((JobKind) jobKindBox.getSelectedItem(),
                    numbersArea.getText(), rangeStartField.getText(), rangeEndField.getText());
            client = currentClient();
        } catch (IllegalArgumentException e) {
            showError(e.getMessage());
            return;
        }

        JobProgressTracker tracker = new JobProgressTracker(job.jobId(), job.kind().name());
        jobsPanel.addJob(tracker);
        logMessage("Submitting " + job.describe() + " as " + job.jobId());
        setStatus("Submitted " + job.jobId());

        // Returns immediately; the outcome arrives on the listener.
        client.submit(job, new GuiJobListener(job, tracker));
    }

    /** Reuses the client for the same host:port, recreates it if they change. */
    private synchronized CoordinatorClient currentClient() {
        String host = hostField.getText().trim();
        if (host.isEmpty()) {
            throw new IllegalArgumentException("Host cannot be empty.");
        }
        int port = JobInputParser.parsePort(portField.getText());
        String address = host + ":" + port;
        if (coordinatorClient == null || !address.equals(clientAddress)) {
            if (coordinatorClient != null) {
                coordinatorClient.close();
            }
            coordinatorClient = clientFactory.create(host, port);
            clientAddress = address;
        }
        return coordinatorClient;
    }

    private synchronized void shutdown() {
        backgroundPool.shutdownNow();
        if (coordinatorClient != null) {
            coordinatorClient.close();
        }
    }

    // ------------------------------------------------------------------
    // Listener: called on network threads, never on the EDT
    // ------------------------------------------------------------------

    private class GuiJobListener implements JobProgressListener {
        private final JobSpec job;
        private final JobProgressTracker tracker;

        GuiJobListener(JobSpec job, JobProgressTracker tracker) {
            this.job = job;
            this.tracker = tracker;
        }

        @Override
        public void onProgress(ProgressSnapshot snapshot) {
            tracker.apply(snapshot);
            jobsPanel.refresh();
        }

        @Override
        public void onCompleted(long result) {
            tracker.complete(result);
            jobsPanel.refresh();
            logMessage("COMPLETED " + job.jobId() + " " + job.describe() + " = " + result);
            setStatus("Completed " + job.jobId());
        }

        @Override
        public void onFailed(String message, boolean retryable) {
            tracker.fail(message);
            jobsPanel.refresh();
            logMessage("FAILED " + job.jobId() + ": " + message);
            setStatus("Job " + job.jobId() + " failed");
        }

        @Override
        public void onRetry(int attempt, int maxAttempts, String reason) {
            tracker.restart(attempt);
            jobsPanel.refresh();
            logMessage("RETRY " + job.jobId() + " (attempt " + attempt + "/" + maxAttempts
                    + "): " + reason);
        }
    }

    // ------------------------------------------------------------------
    // Thread-safe UI helpers
    // ------------------------------------------------------------------

    private void logMessage(String msg) {
        String line = "[" + new SimpleDateFormat("HH:mm:ss.SSS").format(new Date()) + "] " + msg + "\n";
        SwingUtilities.invokeLater(() -> {
            logArea.append(line);
            logArea.setCaretPosition(logArea.getDocument().getLength());
        });
    }

    private void setStatus(String text) {
        SwingUtilities.invokeLater(() -> statusLabel.setText(" " + text));
    }

    private void showError(String message) {
        JOptionPane.showMessageDialog(this, message, "Input Error", JOptionPane.ERROR_MESSAGE);
    }
}
