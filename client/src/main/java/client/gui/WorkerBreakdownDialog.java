package client.gui;

import client.progress.JobProgressTracker;
import client.progress.WorkerProgress;
import java.awt.BorderLayout;
import java.awt.Window;
import java.util.List;
import javax.swing.BorderFactory;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JScrollPane;
import javax.swing.JTable;
import javax.swing.Timer;
import javax.swing.table.AbstractTableModel;

/**
 * Pop-up opened by clicking a job's progress bar. Shows one row per worker
 * sub-task. A Swing Timer re-reads the tracker so the dialog stays live; the
 * timer only repaints, it never changes any progress value.
 */
public class WorkerBreakdownDialog extends JDialog {

    private static final int REFRESH_MS = 250;
    private static final String[] COLUMNS = {"Worker", "Task", "Units done", "Total units", "Progress", "Status"};

    private final JobProgressTracker job;
    private List<WorkerProgress> rows;
    private final JLabel summary = new JLabel();
    private final Timer refreshTimer;

    private final AbstractTableModel model = new AbstractTableModel() {
        @Override public int getRowCount() { return rows.size(); }
        @Override public int getColumnCount() { return COLUMNS.length; }
        @Override public String getColumnName(int c) { return COLUMNS[c]; }
        @Override public Class<?> getColumnClass(int c) { return c == 4 ? Double.class : Object.class; }
        @Override public Object getValueAt(int r, int c) {
            WorkerProgress p = rows.get(r);
            switch (c) {
                case 0: return "Worker " + p.workerId();
                case 1: return p.taskId();
                case 2: return p.unitsCompleted();
                case 3: return p.totalUnits();
                case 4: return p.percent();
                case 5: return p.status();
                default: return "";
            }
        }
    };

    public WorkerBreakdownDialog(Window owner, JobProgressTracker job) {
        super(owner, "Worker breakdown - " + job.getJobId(), ModalityType.MODELESS);
        this.job = job;
        this.rows = job.workerBreakdown();

        JTable table = new JTable(model);
        table.getColumnModel().getColumn(4).setCellRenderer(new ProgressBarRenderer());
        table.setRowHeight(22);

        summary.setBorder(BorderFactory.createEmptyBorder(8, 8, 8, 8));
        add(summary, BorderLayout.NORTH);
        add(new JScrollPane(table), BorderLayout.CENTER);

        refreshTimer = new Timer(REFRESH_MS, e -> reload());
        refreshTimer.start();
        reload();

        setSize(640, 260);
        setLocationRelativeTo(owner);
        setDefaultCloseOperation(DISPOSE_ON_CLOSE);
    }

    private void reload() {
        rows = job.workerBreakdown();
        model.fireTableDataChanged();
        summary.setText(String.format("%s  |  %s  |  overall %.1f%%  |  %d sub-task(s)",
                job.getJobType(), job.getState(), job.overallPercent(), rows.size()));
        if (job.isFinished()) {
            refreshTimer.stop();
        }
    }

    @Override
    public void dispose() {
        refreshTimer.stop();
        super.dispose();
    }
}
