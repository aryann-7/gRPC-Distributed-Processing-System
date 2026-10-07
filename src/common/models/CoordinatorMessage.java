package common.models;

import java.io.Serializable;

/**
 * Data Transfer Object representing status, heartbeat, or state change messages sent by the coordinator.
 */
public class CoordinatorMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    public enum CoordinatorMessageType {
        HEARTBEAT,
        TERM_EXPIRED,
        NEW_COORDINATOR_READY,
        SHUTDOWN
    }

    private String coordinatorId;
    private CoordinatorMessageType type;
    private int currentTerm;
    private int jobAllocationCounter;
    private long timestamp;

    public CoordinatorMessage() {
    }

    public CoordinatorMessage(String coordinatorId, CoordinatorMessageType type, int currentTerm, int jobAllocationCounter, long timestamp) {
        this.coordinatorId = coordinatorId;
        this.type = type;
        this.currentTerm = currentTerm;
        this.jobAllocationCounter = jobAllocationCounter;
        this.timestamp = timestamp;
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public void setCoordinatorId(String coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    public CoordinatorMessageType getType() {
        return type;
    }

    public void setType(CoordinatorMessageType type) {
        this.type = type;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getJobAllocationCounter() {
        return jobAllocationCounter;
    }

    public void setJobAllocationCounter(int jobAllocationCounter) {
        this.jobAllocationCounter = jobAllocationCounter;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
