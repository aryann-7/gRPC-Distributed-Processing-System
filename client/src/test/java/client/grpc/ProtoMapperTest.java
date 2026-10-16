package client.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import client.model.JobKind;
import client.model.JobSpec;
import client.progress.ProgressSnapshot;
import client.progress.WorkerProgress;
import common.grpc.JobProgress;
import common.grpc.JobStateProto;
import common.grpc.JobTypeProto;
import common.grpc.SubmitJobRequest;
import common.grpc.WorkerIdentity;
import common.grpc.WorkerJobProgress;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ProtoMapperTest {

    @Test
    public void listJobUsesTheNumbersBranchOfTheOneof() {
        SubmitJobRequest r = ProtoMapper.toRequest(
                JobSpec.forNumbers("job-1", JobKind.PRIMECOUNT, List.of(2L, 3L, 4L)));
        assertEquals("job-1", r.getJobId());
        assertEquals(JobTypeProto.PRIMECOUNT, r.getJobType());
        assertEquals(SubmitJobRequest.InputCase.NUMBERS, r.getInputCase());
        assertEquals(List.of(2L, 3L, 4L), r.getNumbers().getNumbersList());
        assertFalse(r.hasRange());
    }

    @Test
    public void rangeJobUsesTheRangeBranchOfTheOneof() {
        SubmitJobRequest r = ProtoMapper.toRequest(JobSpec.forRange("job-2", 1, 20));
        assertEquals(JobTypeProto.PRIMESUM, r.getJobType());
        assertEquals(SubmitJobRequest.InputCase.RANGE, r.getInputCase());
        assertEquals(1, r.getRange().getStart());
        assertEquals(20, r.getRange().getEnd());
        assertFalse(r.hasNumbers());
    }

    @Test
    public void jobProgressBecomesASnapshot() {
        ProgressSnapshot s = ProtoMapper.toSnapshot(JobProgress.newBuilder()
                .setJobId("job-3").setState(JobStateProto.RUNNING)
                .setUnitsCompleted(30).setTotalUnits(120)
                .addWorkers(WorkerJobProgress.newBuilder()
                        .setWorker(WorkerIdentity.newBuilder().setWorkerId(102).setSessionId("s-102"))
                        .setUnitsCompleted(30).setTotalUnits(60))
                .build());
        assertFalse(s.queued());
        assertEquals(30, s.unitsCompleted());
        assertEquals(120, s.totalUnits());
        assertEquals(List.of(new WorkerProgress(102, 30, 60)), s.workers());
    }

    @Test
    public void queuedStateIsFlagged() {
        ProgressSnapshot s = ProtoMapper.toSnapshot(JobProgress.newBuilder()
                .setJobId("job-4").setState(JobStateProto.QUEUED).setTotalUnits(10).build());
        assertTrue(s.queued());
        assertTrue(s.workers().isEmpty());
    }
}
