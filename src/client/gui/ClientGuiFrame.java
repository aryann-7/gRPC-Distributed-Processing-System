package client.gui;

import client.parser.CsvDataParser;
import common.interfaces.CoordinatorService;
import common.models.JobRequest;
import common.models.JobResult;
import common.models.JobType;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.io.File;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
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
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;

/**
 * Java Swing GUI Frontend for client job submissions.
 * Accepts manual data entry, PRIMESUM range (start, end), or loading data from CSV files.
 * Supports concurrent execution/submission within a single client and across multiple client processes.
 */
public class ClientGuiFrame extends JFrame {
    private static final long serialVersionUID = 1L;

    private final String clientId;

    // GUI UI Components
    private JTextField coordinatorHostField;
    private JTextField coordinatorPortField;
    private JComboBox<JobType> jobTypeComboBox;
    private JTextArea manualInputArea;    // For MAX and PRIMECOUNT numbers
    private JTextField rangeStartField;   // For PRIMESUM start
    private JTextField rangeEndField;     // For PRIMESUM end
    private JButton browseCsvButton;
    private JButton submitAsyncButton;
    private JTextArea resultsOutputArea;
    private JLabel statusLabel;

    // Dedicated thread pool supporting concurrent submissions within this single client GUI.
    private final ExecutorService clientSubmissionPool = Executors.newCachedThreadPool();

    public ClientGuiFrame() {
        this("1");
    }

    public ClientGuiFrame(String clientId) {
        super("CS324 Cluster Client - #" + clientId);
        this.clientId = clientId;
        initComponents();
    }

    // Initializes GUI components, layouts, and event listeners.
    private void initComponents() {
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setSize(850, 700);
        setMinimumSize(new Dimension(750, 550));
        setLocationRelativeTo(null);

        JPanel mainPanel = new JPanel(new BorderLayout(10, 10));
        mainPanel.setBorder(BorderFactory.createEmptyBorder(12, 12, 12, 12));

        // 1. Connection Panel (Top)
        JPanel connectionPanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 5));
        connectionPanel.setBorder(BorderFactory.createTitledBorder("Coordinator Discovery & Endpoint"));

        connectionPanel.add(new JLabel("Host:"));
        coordinatorHostField = new JTextField("localhost", 12);
        connectionPanel.add(coordinatorHostField);

        connectionPanel.add(new JLabel("Port:"));
        coordinatorPortField = new JTextField("1099", 6);
        connectionPanel.add(coordinatorPortField);

        JButton testConnButton = new JButton("Test Discovery");
        testConnButton.addActionListener(e -> testCoordinatorConnection());
        connectionPanel.add(testConnButton);

        mainPanel.add(connectionPanel, BorderLayout.NORTH);

        // 2. Middle Panel: Input Configuration & Controls
        JPanel centerPanel = new JPanel(new GridBagLayout());
        GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.BOTH;
        gbc.insets = new Insets(5, 5, 5, 5);

        // Row 0: Job Type Selection
        gbc.gridx = 0;
        gbc.gridy = 0;
        gbc.weightx = 0.2;
        gbc.weighty = 0.0;
        centerPanel.add(new JLabel("Job Type:"), gbc);

        jobTypeComboBox = new JComboBox<>(JobType.values());
        jobTypeComboBox.addActionListener(e -> updateInputModeVisibility());
        gbc.gridx = 1;
        gbc.weightx = 0.8;
        centerPanel.add(jobTypeComboBox, gbc);

        // Row 1: Manual List Input (MAX & PRIMECOUNT)
        gbc.gridx = 0;
        gbc.gridy = 1;
        gbc.weightx = 0.2;
        gbc.weighty = 0.4;
        centerPanel.add(new JLabel("Number List (MAX / PRIMECOUNT):"), gbc);

        manualInputArea = new JTextArea("10, 25, 3, 99, 17, 4, 101, 88, 2, 7", 5, 30);
        manualInputArea.setLineWrap(true);
        manualInputArea.setWrapStyleWord(true);
        JScrollPane manualInputScroll = new JScrollPane(manualInputArea);
        gbc.gridx = 1;
        gbc.weightx = 0.8;
        centerPanel.add(manualInputScroll, gbc);

        // Row 2: CSV Browse row
        gbc.gridx = 0;
        gbc.gridy = 2;
        gbc.weightx = 0.2;
        gbc.weighty = 0.0;
        centerPanel.add(new JLabel("Or Load CSV:"), gbc);

        browseCsvButton = new JButton("Browse CSV File...");
        browseCsvButton.addActionListener(e -> handleCsvBrowse());
        gbc.gridx = 1;
        gbc.weightx = 0.8;
        centerPanel.add(browseCsvButton, gbc);

        // Row 3: Range Inputs (PRIMESUM)
        gbc.gridx = 0;
        gbc.gridy = 3;
        gbc.weightx = 0.2;
        gbc.weighty = 0.0;
        centerPanel.add(new JLabel("Range (PRIMESUM):"), gbc);

        JPanel rangePanel = new JPanel(new FlowLayout(FlowLayout.LEFT, 10, 0));
        rangePanel.add(new JLabel("Start:"));
        rangeStartField = new JTextField("1", 10);
        rangePanel.add(rangeStartField);
        rangePanel.add(new JLabel("End:"));
        rangeEndField = new JTextField("1000", 10);
        rangePanel.add(rangeEndField);

        gbc.gridx = 1;
        gbc.weightx = 0.8;
        centerPanel.add(rangePanel, gbc);

        // Row 4: Submit Button
        submitAsyncButton = new JButton("Submit Job (Async)");
        submitAsyncButton.setFont(new Font("SansSerif", Font.BOLD, 13));
        submitAsyncButton.addActionListener(e -> handleAsyncJobSubmission());
        gbc.gridx = 0;
        gbc.gridy = 4;
        gbc.gridwidth = 2;
        gbc.weighty = 0.0;
        centerPanel.add(submitAsyncButton, gbc);

        // Row 5: Results Area
        gbc.gridx = 0;
        gbc.gridy = 5;
        gbc.gridwidth = 2;
        gbc.weighty = 0.6;
        resultsOutputArea = new JTextArea();
        resultsOutputArea.setEditable(false);
        resultsOutputArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        JScrollPane resultsScroll = new JScrollPane(resultsOutputArea);
        resultsScroll.setBorder(BorderFactory.createTitledBorder("Execution Log & Results"));
        centerPanel.add(resultsScroll, gbc);

        mainPanel.add(centerPanel, BorderLayout.CENTER);

        // 3. Status Bar (Bottom)
        JPanel bottomPanel = new JPanel(new BorderLayout());
        statusLabel = new JLabel(" Ready (Client #" + clientId + ")");
        statusLabel.setForeground(new Color(0, 100, 0));
        bottomPanel.add(statusLabel, BorderLayout.WEST);
        mainPanel.add(bottomPanel, BorderLayout.SOUTH);

        setContentPane(mainPanel);
        updateInputModeVisibility();
    }

    private void updateInputModeVisibility() {
        JobType selected = (JobType) jobTypeComboBox.getSelectedItem();
        boolean isRange = (selected == JobType.PRIMESUM);
        rangeStartField.setEnabled(isRange);
        rangeEndField.setEnabled(isRange);
        manualInputArea.setEnabled(!isRange);
        browseCsvButton.setEnabled(!isRange);
    }

    private void testCoordinatorConnection() {
        String host = coordinatorHostField.getText().trim();
        int port;
        try {
            port = Integer.parseInt(coordinatorPortField.getText().trim());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this, "Port must be a valid integer.", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        clientSubmissionPool.submit(() -> {
            logMessage("Testing discovery of CoordinatorService at " + host + ":" + port + "...");
            CoordinatorService coord = lookupCoordinator(host, port);
            if (coord != null) {
                logMessage("SUCCESS: Coordinator discovered and reachable at " + host + ":" + port);
                SwingUtilities.invokeLater(() -> statusLabel.setText(" Coordinator reachable"));
            } else {
                logMessage("ERROR: Could not locate active CoordinatorService at " + host + ":" + port);
                SwingUtilities.invokeLater(() -> statusLabel.setText(" Coordinator not found"));
            }
        });
    }

    // Opens a file chooser dialog to load numbers from a CSV file.
    private void handleCsvBrowse() {
        JFileChooser chooser = new JFileChooser();
        chooser.setDialogTitle("Select CSV dataset file");
        int res = chooser.showOpenDialog(this);
        if (res == JFileChooser.APPROVE_OPTION) {
            File selectedFile = chooser.getSelectedFile();
            try {
                List<Long> nums = CsvDataParser.parseCsv(selectedFile);
                if (nums.isEmpty()) {
                    JOptionPane.showMessageDialog(this, "No valid numbers found in CSV file.", "Info", JOptionPane.INFORMATION_MESSAGE);
                    return;
                }
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < nums.size(); i++) {
                    sb.append(nums.get(i));
                    if (i < nums.size() - 1) {
                        sb.append(", ");
                    }
                }
                manualInputArea.setText(sb.toString());
                logMessage("Loaded " + nums.size() + " data points from " + selectedFile.getName());
            } catch (Exception e) {
                JOptionPane.showMessageDialog(this, "Failed to read CSV: " + e.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
            }
        }
    }

    // Submits a computation job concurrently to the coordinator to prevent UI freezing.
    private void handleAsyncJobSubmission() {
        String host = coordinatorHostField.getText().trim();
        int port;
        try {
            port = Integer.parseInt(coordinatorPortField.getText().trim());
        } catch (NumberFormatException e) {
            JOptionPane.showMessageDialog(this, "Invalid port number", "Error", JOptionPane.ERROR_MESSAGE);
            return;
        }

        JobType type = (JobType) jobTypeComboBox.getSelectedItem();
        String jobId = "job-" + UUID.randomUUID().toString().substring(0, 8);

        JobRequest request;
        if (type == JobType.PRIMESUM) {
            try {
                long start = Long.parseLong(rangeStartField.getText().trim());
                long end = Long.parseLong(rangeEndField.getText().trim());
                if (start > end) {
                    JOptionPane.showMessageDialog(this, "Start value cannot exceed End value.", "Input Error", JOptionPane.ERROR_MESSAGE);
                    return;
                }
                request = new JobRequest(jobId, type, start, end);
            } catch (NumberFormatException e) {
                JOptionPane.showMessageDialog(this, "Start and End must be valid integer/long numbers.", "Input Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
        } else {
            String rawText = manualInputArea.getText().trim();
            if (rawText.isEmpty()) {
                JOptionPane.showMessageDialog(this, "Input list cannot be empty for " + type, "Input Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
            List<Long> numbers = new ArrayList<>();
            String[] tokens = rawText.split("[,\\s]+");
            for (String tok : tokens) {
                tok = tok.trim();
                if (!tok.isEmpty()) {
                    try {
                        numbers.add(Long.parseLong(tok));
                    } catch (NumberFormatException e) {
                        JOptionPane.showMessageDialog(this, "Invalid number: '" + tok + "'", "Input Error", JOptionPane.ERROR_MESSAGE);
                        return;
                    }
                }
            }
            if (numbers.isEmpty()) {
                JOptionPane.showMessageDialog(this, "No valid numbers entered.", "Input Error", JOptionPane.ERROR_MESSAGE);
                return;
            }
            request = new JobRequest(jobId, type, numbers);
        }

        statusLabel.setText(" Submitting " + jobId + "...");
        logMessage("Submitting " + request.getJobType() + " (" + jobId + ") to coordinator...");

        // Dispatch via dedicated client thread pool with retry tolerance for coordinator re-elections
        clientSubmissionPool.submit(() -> {
            JobResult result = null;
            Exception lastException = null;

            for (int attempt = 1; attempt <= 3; attempt++) {
                CoordinatorService coordinator = lookupCoordinator(host, port);
                if (coordinator == null) {
                    if (attempt < 3) {
                        logMessage("RETRY [" + jobId + "]: Coordinator in transition/re-election, retrying in 1.5s (attempt " + attempt + "/3)...");
                        try {
                            Thread.sleep(1500);
                        } catch (InterruptedException ignored) {}
                        continue;
                    }
                    logMessage("ERROR [" + jobId + "]: Cannot reach active coordinator at " + host + ":" + port);
                    SwingUtilities.invokeLater(() -> statusLabel.setText(" Failed to reach coordinator"));
                    return;
                }

                try {
                    result = coordinator.submitJob(request);
                    if (result != null && result.isSuccess()) {
                        break;
                    } else if (result != null && result.getErrorMessage() != null && result.getErrorMessage().contains("Election required")) {
                        if (attempt < 3) {
                            logMessage("RETRY [" + jobId + "]: Coordinator term ended, waiting for new coordinator election...");
                            try {
                                Thread.sleep(2000);
                            } catch (InterruptedException ignored) {}
                            continue;
                        }
                    }
                    break;
                } catch (Exception e) {
                    lastException = e;
                    if (attempt < 3) {
                        logMessage("RETRY [" + jobId + "]: Communication interrupted (" + e.getMessage() + "), retrying in 1.5s...");
                        try {
                            Thread.sleep(1500);
                        } catch (InterruptedException ignored) {}
                    }
                }
            }

            final JobResult evaluatedResult = result;
            if (evaluatedResult != null) {
                if (evaluatedResult.isSuccess()) {
                    logMessage("COMPLETED [" + jobId + "]: Result = " + evaluatedResult.getResultValue() + " (Elapsed: " + evaluatedResult.getExecutionTimeMs() + " ms)");
                    SwingUtilities.invokeLater(() -> statusLabel.setText(" Completed " + jobId + " in " + evaluatedResult.getExecutionTimeMs() + "ms"));
                } else {
                    logMessage("FAILED [" + jobId + "]: " + evaluatedResult.getErrorMessage());
                    SwingUtilities.invokeLater(() -> statusLabel.setText(" Job " + jobId + " failed"));
                }
            } else if (lastException != null) {
                logMessage("COMMUNICATION ERROR [" + jobId + "]: " + lastException.getMessage());
                SwingUtilities.invokeLater(() -> statusLabel.setText(" Error submitting " + jobId));
            }
        });
    }

    // Locates the active coordinator remote service in the RMI registry.
    private CoordinatorService lookupCoordinator(String host, int port) {
        try {
            Registry registry = LocateRegistry.getRegistry(host, port);
            return (CoordinatorService) registry.lookup("CoordinatorService");
        } catch (Exception e) {
            return null;
        }
    }

    private void logMessage(String msg) {
        String timestamp = new SimpleDateFormat("HH:mm:ss.SSS").format(new Date());
        SwingUtilities.invokeLater(() -> {
            resultsOutputArea.append("[" + timestamp + "] " + msg + "\n");
            resultsOutputArea.setCaretPosition(resultsOutputArea.getDocument().getLength());
        });
    }
}
