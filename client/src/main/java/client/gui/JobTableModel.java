package client.gui;

import client.progress.JobProgressTracker;
import java.util.ArrayList;
import java.util.List;
import javax.swing.table.AbstractTableModel;

/**
 * One row per submitted job: ID, type, progress bar, status, result.
 * Must only be touched on the Swing Event Dispatch Thread.
 */
public class JobTableModel extends AbstractTableModel {

    private static final String[] COLUMNS = {"Job ID", "Type", "Status", "Result"};

    private final List<JobProgressTracker> jobs = new ArrayList<>();

    public void addJob(JobProgressTracker job) {
        jobs.add(job);
        fireTableRowsInserted(jobs.size() - 1, jobs.size() - 1);
    }

    /** Repaints all rows; call after progress changes. */
    public void refresh() {
        if (!jobs.isEmpty()) {
            fireTableRowsUpdated(0, jobs.size() - 1);
        }
    }

    public JobProgressTracker getJob(int row) {
        return jobs.get(row);
    }

    @Override public int getRowCount() { return jobs.size(); }
    @Override public int getColumnCount() { return COLUMNS.length; }
    @Override public String getColumnName(int col) { return COLUMNS[col]; }

    @Override
    public Class<?> getColumnClass(int col) {
        return String.class;
    }

    @Override
    public Object getValueAt(int row, int col) {
        JobProgressTracker job = jobs.get(row);
        switch (col) {
            case 0: return job.getJobId();
            case 1: return job.getJobType();
            case 2:
                return job.getAttempt() > 1
                        ? job.getState() + " (attempt " + job.getAttempt() + ")"
                        : job.getState().toString();
            case 3:
                if (job.getState() == JobProgressTracker.State.FAILED) {
                    return job.getErrorMessage();
                }
                return job.getResult() == null ? "" : String.valueOf(job.getResult());
            default: return "";
        }
    }
}
