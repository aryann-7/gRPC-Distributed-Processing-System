package common.models;

import java.io.Serializable;
import java.util.List;

/**
 * Data Transfer Object representing a sub-task allocated to an individual worker node.
 * Coordinator splits workload evenly across available workers.
 */
public class JobTask implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobId;
    private String taskId;
    private JobType jobType;
    private List<Long> dataChunk; // For list-based partitioning (MAX, PRIMECOUNT)
    private long rangeStart;      // For range-based partitioning (PRIMESUM)
    private long rangeEnd;        // For range-based partitioning (PRIMESUM)

    public JobTask() {
    }

    public JobTask(String jobId, String taskId, JobType jobType, List<Long> dataChunk) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.jobType = jobType;
        this.dataChunk = dataChunk;
    }

    public JobTask(String jobId, String taskId, JobType jobType, long rangeStart, long rangeEnd) {
        this.jobId = jobId;
        this.taskId = taskId;
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

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public JobType getJobType() {
        return jobType;
    }

    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }

    public List<Long> getDataChunk() {
        return dataChunk;
    }

    public void setDataChunk(List<Long> dataChunk) {
        this.dataChunk = dataChunk;
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

