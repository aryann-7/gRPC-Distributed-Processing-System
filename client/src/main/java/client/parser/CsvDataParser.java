package client.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

// Utility class for parsing input datasets from CSV files.

public class CsvDataParser {

    /**
     * Parses numeric data points from a CSV file.
     *
     * @param file CSV file containing numerical records.
     * @return List of parsed Long values.
     * @throws IOException if file reading fails.
     */
    public static List<Long> parseCsv(File file) throws IOException {
        if (file == null || !file.exists()) {
            throw new IOException("File does not exist or is null");
        }

        List<Long> numbers = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] tokens = line.split("[,\\s]+");
                for (String token : tokens) {
                    token = token.trim();
                    if (!token.isEmpty()) {
                        try {
                            numbers.add(Long.parseLong(token));
                        } catch (NumberFormatException ignored) {
                            // Skip non-numeric header tokens or invalid entries
                        }
                    }
                }
            }
        }
        return numbers;
    }
}
