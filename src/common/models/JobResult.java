package common.models;

import java.io.Serializable;

/**
 * Data Transfer Object representing the result of a computed job or sub-task.
 */
public class JobResult implements Serializable {
    private static final long serialVersionUID = 1L;

    private String jobId;
    private String taskId;
    private boolean success;
    private long resultValue;
    private String errorMessage;
    private long executionTimeMs;

    public JobResult() {
    }

    public JobResult(String jobId, String taskId, boolean success, long resultValue, String errorMessage, long executionTimeMs) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.success = success;
        this.resultValue = resultValue;
        this.errorMessage = errorMessage;
        this.executionTimeMs = executionTimeMs;
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

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public long getResultValue() {
        return resultValue;
    }

    public void setResultValue(long resultValue) {
        this.resultValue = resultValue;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }

    public void setExecutionTimeMs(long executionTimeMs) {
        this.executionTimeMs = executionTimeMs;
    }
}
