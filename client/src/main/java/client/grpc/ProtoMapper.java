package client.grpc;

import client.model.JobKind;
import client.model.JobSpec;
import client.progress.WorkerProgress;
import common.grpc.client.ClientJobType;
import common.grpc.client.JobSubmission;
import common.grpc.client.WorkerTaskProgress;

/**
 * Converts between the client's own classes and the generated protobuf
 * classes. This is the only place that knows the proto field names, so a
 * change to client.proto only affects this file and GrpcCoordinatorClient.
 */
public final class ProtoMapper {

    private ProtoMapper() {
    }

    /** JobSpec (client) -> JobSubmission (wire). */
    public static JobSubmission toSubmission(JobSpec job, String clientId) {
        JobSubmission.Builder builder = JobSubmission.newBuilder()
                .setJobId(job.jobId())
                .setJobType(toProto(job.kind()))
                .setClientId(clientId);
        if (job.kind().isRangeBased()) {
            builder.setRangeStart(job.rangeStart()).setRangeEnd(job.rangeEnd());
        } else {
            builder.addAllNumbers(job.numbers());
        }
        return builder.build();
    }

    public static ClientJobType toProto(JobKind kind) {
        switch (kind) {
            case MAX: return ClientJobType.MAX;
            case PRIMESUM: return ClientJobType.PRIMESUM;
            case PRIMECOUNT: return ClientJobType.PRIMECOUNT;
            default: throw new IllegalArgumentException("Unknown job kind: " + kind);
        }
    }

    /** WorkerTaskProgress (wire) -> WorkerProgress (client). */
    public static WorkerProgress fromProto(WorkerTaskProgress p) {
        return new WorkerProgress(p.getWorkerId(), p.getTaskId(),
                p.getUnitsCompleted(), p.getTotalUnits(), p.getStatus());
    }
}
