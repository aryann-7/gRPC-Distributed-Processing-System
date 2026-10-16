package client.grpc;

import client.model.JobKind;
import client.model.JobSpec;
import client.progress.ProgressSnapshot;
import client.progress.WorkerProgress;
import common.grpc.JobNumbers;
import common.grpc.JobProgress;
import common.grpc.JobRange;
import common.grpc.JobStateProto;
import common.grpc.JobTypeProto;
import common.grpc.SubmitJobRequest;
import common.grpc.WorkerJobProgress;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts between the client's own classes and the generated protobuf
 * classes from the team's coordinator.proto / worker.proto.
 *
 * This is the only place that knows the message field names, so a change
 * to those .proto files only affects this file and GrpcCoordinatorClient.
 */
public final class ProtoMapper {

    private ProtoMapper() {
    }

    /**
     * JobSpec (client) -> SubmitJobRequest (wire).
     * The request's "input" is a oneof: a number list for MAX / PRIMECOUNT,
     * or an inclusive range for PRIMESUM.
     */
    public static SubmitJobRequest toRequest(JobSpec job) {
        SubmitJobRequest.Builder builder = SubmitJobRequest.newBuilder()
                .setJobId(job.jobId())
                .setJobType(toProto(job.kind()));
        if (job.kind().isRangeBased()) {
            builder.setRange(JobRange.newBuilder()
                    .setStart(job.rangeStart())
                    .setEnd(job.rangeEnd()));
        } else {
            builder.setNumbers(JobNumbers.newBuilder().addAllNumbers(job.numbers()));
        }
        return builder.build();
    }

    public static JobTypeProto toProto(JobKind kind) {
        switch (kind) {
            case MAX: return JobTypeProto.MAX;
            case PRIMESUM: return JobTypeProto.PRIMESUM;
            case PRIMECOUNT: return JobTypeProto.PRIMECOUNT;
            default: throw new IllegalArgumentException("Unknown job kind: " + kind);
        }
    }

    /** JobProgress (wire) -> ProgressSnapshot (client). */
    public static ProgressSnapshot toSnapshot(JobProgress message) {
        List<WorkerProgress> workers = new ArrayList<>(message.getWorkersCount());
        for (WorkerJobProgress w : message.getWorkersList()) {
            workers.add(fromProto(w));
        }
        return new ProgressSnapshot(message.getState() == JobStateProto.QUEUED,
                message.getUnitsCompleted(), message.getTotalUnits(), workers);
    }

    /** WorkerJobProgress (wire) -> WorkerProgress (client). */
    public static WorkerProgress fromProto(WorkerJobProgress w) {
        return new WorkerProgress(w.getWorker().getWorkerId(),
                w.getUnitsCompleted(), w.getTotalUnits());
    }
}
