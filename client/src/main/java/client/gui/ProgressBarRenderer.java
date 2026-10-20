package client.gui;
 
import java.awt.Component;
import javax.swing.JProgressBar;
import javax.swing.JTable;
import javax.swing.table.TableCellRenderer;
 
/**
 * Visual progress bar renderer for percentage cells, displaying both the
 * graphical progress bar and the current text percentage moving together.
 * Stops and freezes at the exact value if a job is cancelled or fails.
 */
public class ProgressBarRenderer extends JProgressBar implements TableCellRenderer {
 
    public ProgressBarRenderer() {
        super(0, 100);
        setStringPainted(true);
        setBorderPainted(true);
    }
 
    @Override
    public Component getTableCellRendererComponent(JTable table, Object value,
            boolean isSelected, boolean hasFocus, int row, int column) {
        double percent = 0.0;
        if (value instanceof Number n) {
            percent = n.doubleValue();
        }
        int intVal = (int) Math.round(percent);
        setValue(Math.max(0, Math.min(100, intVal)));
        setString(String.format("%.1f%%", percent));
 
        if (isSelected) {
            setBackground(table.getSelectionBackground());
        } else {
            setBackground(table.getBackground());
        }
        return this;
    }
}
