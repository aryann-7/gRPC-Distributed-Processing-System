package client.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import client.model.JobKind;
import client.model.JobSpec;
import client.progress.WorkerProgress;
import common.grpc.client.ClientJobType;
import common.grpc.client.JobSubmission;
import common.grpc.client.WorkerTaskProgress;
import java.util.List;
import org.junit.jupiter.api.Test;

public class ProtoMapperTest {

    @Test
    public void listJobCarriesNumbersAndNoRange() {
        JobSubmission s = ProtoMapper.toSubmission(
                JobSpec.forNumbers("job-1", JobKind.PRIMECOUNT, List.of(2L, 3L, 4L)), "c1");
        assertEquals("job-1", s.getJobId());
        assertEquals(ClientJobType.PRIMECOUNT, s.getJobType());
        assertEquals(List.of(2L, 3L, 4L), s.getNumbersList());
        assertEquals(0, s.getRangeEnd());
        assertEquals("c1", s.getClientId());
    }

    @Test
    public void rangeJobCarriesRangeAndNoNumbers() {
        JobSubmission s = ProtoMapper.toSubmission(JobSpec.forRange("job-2", 1, 20), "c1");
        assertEquals(ClientJobType.PRIMESUM, s.getJobType());
        assertEquals(1, s.getRangeStart());
        assertEquals(20, s.getRangeEnd());
        assertEquals(0, s.getNumbersCount());
    }

    @Test
    public void workerProgressIsCopiedFieldByField() {
        WorkerProgress p = ProtoMapper.fromProto(WorkerTaskProgress.newBuilder()
                .setWorkerId(102).setTaskId("t2").setUnitsCompleted(30).setTotalUnits(60)
                .setStatus("PROCESSING").build());
        assertEquals(new WorkerProgress(102, "t2", 30, 60, "PROCESSING"), p);
    }
}
