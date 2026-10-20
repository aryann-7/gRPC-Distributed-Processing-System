package client.gui;

import java.awt.Component;
import javax.swing.JTable;
import javax.swing.SwingConstants;
import javax.swing.table.DefaultTableCellRenderer;

/**
 * Clean text renderer for percentage cells, displaying values as "XX.X%".
 * Avoids heavy or platform-dependent graphical progress bars.
 */
public class ProgressBarRenderer extends DefaultTableCellRenderer {

    public ProgressBarRenderer() {
        setHorizontalAlignment(SwingConstants.CENTER);
    }

    @Override
    public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
        super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);
        if (value instanceof Double percent) {
            setText(String.format("%.1f%%", percent));
        }
        return this;
    }
}
