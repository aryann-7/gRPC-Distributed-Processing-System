package client.model;

import java.util.List;
import java.util.Objects;

/**
 * Everything the client knows about one job it wants to run.
 *
 * Immutable, so it is safe to resubmit the same object on a retry: the job
 * ID stays the same, which lets the coordinator recognise a resubmission.
 */
public record JobSpec(String jobId, JobKind kind, List<Long> numbers,
                      long rangeStart, long rangeEnd) {

    public JobSpec {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(kind, "kind");
        numbers = (numbers == null) ? List.of() : List.copyOf(numbers);
    }

    /** MAX or PRIMECOUNT over a list of numbers. */
    public static JobSpec forNumbers(String jobId, JobKind kind, List<Long> numbers) {
        if (kind.isRangeBased()) {
            throw new IllegalArgumentException(kind + " takes a range, not a number list");
        }
        if (numbers == null || numbers.isEmpty()) {
            throw new IllegalArgumentException("No valid numbers entered.");
        }
        return new JobSpec(jobId, kind, numbers, 0, 0);
    }

    /** PRIMESUM over [start, end]. */
    public static JobSpec forRange(String jobId, long start, long end) {
        if (start > end) {
            throw new IllegalArgumentException("Start value cannot exceed End value.");
        }
        return new JobSpec(jobId, JobKind.PRIMESUM, List.of(), start, end);
    }

    /** Short human-readable summary for the log. */
    public String describe() {
        return kind.isRangeBased()
                ? kind + "(" + rangeStart + ", " + rangeEnd + ")"
                : kind + "(" + numbers.size() + " numbers)";
    }
}
