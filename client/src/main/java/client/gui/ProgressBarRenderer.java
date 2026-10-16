package client.gui;

import java.awt.Component;
import javax.swing.JProgressBar;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;

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
        bar.setString(String.format("%.1f%%", percent));
        return bar;
    }
}
