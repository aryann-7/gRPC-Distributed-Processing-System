package client.gui;

import java.awt.Color;
import java.awt.Component;
import javax.swing.JProgressBar;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;
import client.progress.JobProgressTracker;

/** Draws a table cell's Double value (0-100) as a progress bar. */
public class ProgressBarRenderer implements TableCellRenderer {

    private final JProgressBar bar = new JProgressBar(0, 100);

    public ProgressBarRenderer() {
        bar.setStringPainted(true);
    }

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
        double percent = (value instanceof Double) ? (Double) value : 0.0;
        bar.setValue((int) Math.round(percent));

        // Check if the underlying row represents an aborted/failed/cancelled task
        boolean aborted = false;
        String statusText = null;
        if (table.getModel() instanceof JobTableModel jobModel && row >= 0 && row < jobModel.getRowCount()) {
            JobProgressTracker job = jobModel.getJob(table.convertRowIndexToModel(row));
            if (job != null) {
                if (job.getState() == JobProgressTracker.State.FAILED) {
                    aborted = true;
                    statusText = (job.getErrorMessage() != null && job.getErrorMessage().toUpperCase().contains("CANCEL"))
                            ? String.format("%.1f%% (CANCELLED)", percent)
                            : String.format("%.1f%% (ABORTED)", percent);
                }
            }
        }

        if (aborted) {
            bar.setForeground(new Color(200, 50, 50));
            bar.setString(statusText);
        } else {
            bar.setForeground(null); // default UI progress bar color
            bar.setString(String.format("%.1f%%", percent));
        }

        return bar;
    }
}
