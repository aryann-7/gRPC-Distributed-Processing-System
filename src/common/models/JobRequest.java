package common.models;

import java.io.Serializable;
import java.util.List;

/**
 * Data Transfer Object representing a job submitted by a client to the coordinator.
 * Supports:
 * - MAX(numbers): Largest value from an unsorted list.
 * - PRIMESUM(start, end): Sum of primes within range [start, end].
 * - PRIMECOUNT(numbers): Count of primes in an unsorted list.
 */
public class JobRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobId;
    private JobType jobType;
    private List<Long> numbers; // Used by MAX and PRIMECOUNT
    private long rangeStart;    // Used by PRIMESUM
    private long rangeEnd;      // Used by PRIMESUM

    public JobRequest() {
    }

    /**
     * Constructor for list-based jobs: MAX(numbers) and PRIMECOUNT(numbers).
     */
    public JobRequest(String jobId, JobType jobType, List<Long> numbers) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.numbers = numbers;
    }

    /**
     * Constructor for range-based jobs: PRIMESUM(start, end).
     */
    public JobRequest(String jobId, JobType jobType, long rangeStart, long rangeEnd) {
        this.jobId = jobId;
        this.jobType = jobType;
        this.rangeStart = rangeStart;
        this.rangeEnd = rangeEnd;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    public List<Long> getNumbers() {
        return numbers;
    }

    public void setNumbers(List<Long> numbers) {
        this.numbers = numbers;
    }

    public long getRangeStart() {
        return rangeStart;
    }

    public void setRangeStart(long rangeStart) {
        this.rangeStart = rangeStart;
    }

    public long getRangeEnd() {
        return rangeEnd;
    }

    public void setRangeEnd(long rangeEnd) {
        this.rangeEnd = rangeEnd;
    }
}

