package client.model;

/**
 * The three job types the client can submit.
 *
 * The client keeps its own enum instead of using common.models.JobType or a
 * generated protobuf enum, so the GUI does not change when the transport
 * (RMI now, gRPC later) changes. Each transport adapter maps this enum to its
 * own wire type.
 */
public enum JobKind {
    MAX(false),
    PRIMESUM(true),
    PRIMECOUNT(false);

    private final boolean rangeBased;

    JobKind(boolean rangeBased) {
        this.rangeBased = rangeBased;
    }

    /** True for PRIMESUM (takes start/end), false for list-based jobs. */
    public boolean isRangeBased() {
        return rangeBased;
    }
}
