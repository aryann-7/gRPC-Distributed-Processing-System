package client.model;

/**
 * The three job types the client can submit.
 *
 * The client keeps its own enum instead of using the generated protobuf
 * enum (JobTypeProto), so the GUI does not change if the .proto files change.
 * ProtoMapper maps this enum to the wire type.
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
