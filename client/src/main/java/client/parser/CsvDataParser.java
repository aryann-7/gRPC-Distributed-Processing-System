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

        byte[] bytes = java.nio.file.Files.readAllBytes(file.toPath());
        List<Long> numbers = new ArrayList<>(Math.max(16, bytes.length / 6));

        long current = 0;
        boolean inNumber = false;
        boolean negative = false;
        boolean inComment = false;

        for (int i = 0; i < bytes.length; i++) {
            byte b = bytes[i];

            // Handle newline reset
            if (b == '\n' || b == '\r') {
                if (inNumber) {
                    numbers.add(negative ? -current : current);
                    current = 0;
                    inNumber = false;
                    negative = false;
                }
                inComment = false;
                continue;
            }

            if (inComment) {
                continue;
            }

            // Comment line
            if (b == '#' && !inNumber) {
                inComment = true;
                continue;
            }

            // Digit parsing
            if (b >= '0' && b <= '9') {
                current = current * 10 + (b - '0');
                inNumber = true;
            } else if (b == '-' && !inNumber) {
                negative = true;
                inNumber = true;
            } else {
                if (inNumber) {
                    numbers.add(negative ? -current : current);
                    current = 0;
                    inNumber = false;
                    negative = false;
                }
            }
        }

        if (inNumber) {
            numbers.add(negative ? -current : current);
        }

        return numbers;
    }
}
