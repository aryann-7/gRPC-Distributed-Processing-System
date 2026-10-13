package client.model;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Turns what the user typed into a JobSpec, or explains what is wrong.
 *
 * Kept separate from the GUI so the rules can be unit-tested without Swing.
 * Typed input is strict (any bad token is rejected and named), unlike the
 * CSV parser, which skips headers and junk in files.
 */
public final class JobInputParser {

    private JobInputParser() {
    }

    /**
     * New job ID: "job-" plus a full random UUID. coordinator.proto asks for a
     * client-generated ID (preferably a full UUID) that is reused on retries,
     * so IDs from several clients never clash.
     */
    public static String newJobId() {
        return "job-" + UUID.randomUUID();
    }

    /** Parses "10, 25 3,99" style input. Throws with the first bad token named. */
    public static List<Long> parseNumbers(String text) {
        String raw = (text == null) ? "" : text.trim();
        if (raw.isEmpty()) {
            throw new IllegalArgumentException("Input list cannot be empty.");
        }
        List<Long> numbers = new ArrayList<>();
        for (String token : raw.split("[,\\s]+")) {
            if (token.isEmpty()) {
                continue;
            }
            try {
                numbers.add(Long.parseLong(token));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid number: '" + token + "'");
            }
        }
        if (numbers.isEmpty()) {
            throw new IllegalArgumentException("No valid numbers entered.");
        }
        return numbers;
    }

    /** Parses a whole number field, naming the field in the error. */
    public static long parseLong(String text, String fieldName) {
        try {
            return Long.parseLong(text == null ? "" : text.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(fieldName + " must be a whole number.");
        }
    }

    /** Parses a port number (1-65535). */
    public static int parsePort(String text) {
        long port = parseLong(text, "Port");
        if (port < 1 || port > 65535) {
            throw new IllegalArgumentException("Port must be between 1 and 65535.");
        }
        return (int) port;
    }

    /** Builds a validated JobSpec from the form fields. */
    public static JobSpec build(JobKind kind, String numbersText, String startText, String endText) {
        String jobId = newJobId();
        if (kind.isRangeBased()) {
            long start = parseLong(startText, "Start");
            long end = parseLong(endText, "End");
            return JobSpec.forRange(jobId, start, end);
        }
        return JobSpec.forNumbers(jobId, kind, parseNumbers(numbersText));
    }
}
