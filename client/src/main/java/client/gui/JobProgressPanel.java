package client.gui;

import client.progress.JobProgressTracker;
import java.awt.BorderLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.BorderFactory;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.ListSelectionModel;
import javax.swing.SwingUtilities;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Panel listing every submitted job with a live progress bar.
 * Clicking a row opens that job's worker-by-worker breakdown.
 */
public class JobProgressPanel extends JPanel {

    private final JobTableModel model = new JobTableModel();
    private final JTable table = new JTable(model);
    // Many progress updates can arrive per second; queue at most one repaint at a time.
    private final AtomicBoolean refreshQueued = new AtomicBoolean(false);

    public JobProgressPanel() {
        super(new BorderLayout());
        setBorder(BorderFactory.createTitledBorder("Jobs (click a row for the worker breakdown)"));

        table.setRowHeight(24);
        table.getColumnModel().getColumn(2).setCellRenderer(new ProgressBarRenderer());
        table.getColumnModel().getColumn(2).setPreferredWidth(140);
        table.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
        table.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseClicked(MouseEvent e) {
                int row = table.rowAtPoint(e.getPoint());
                if (row >= 0) {
                    showBreakdown(model.getJob(table.convertRowIndexToModel(row)));
                }
            }
        });

        add(new JScrollPane(table), BorderLayout.CENTER);
    }

    /** Adds a job row. Safe to call from any thread. */
    public void addJob(JobProgressTracker job) {
        SwingUtilities.invokeLater(() -> model.addJob(job));
    }

    /** Repaints progress after an update. Safe to call from any thread. */
    public void refresh() {
        if (refreshQueued.compareAndSet(false, true)) {
            SwingUtilities.invokeLater(() -> {
                refreshQueued.set(false);
                model.refresh();
            });
        }
    }

    private void showBreakdown(JobProgressTracker job) {
        new WorkerBreakdownDialog(SwingUtilities.getWindowAncestor(this), job).setVisible(true);
    }
}
