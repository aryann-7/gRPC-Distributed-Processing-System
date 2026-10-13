package protocol;

import common.grpc.JobTypeProto;
import common.grpc.TaskDispatchRequest;
import common.grpc.TaskProgressUpdate;
import common.grpc.TaskResultResponse;
import common.grpc.WorkerServiceProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.Test;
import worker.computation.GranularJobRunner;

import static org.junit.jupiter.api.Assertions.*;

public class WorkerProtocolCompatibilityTest {

    @Test
    public void legacyProgressReaderPreservesTerminalResultAndRetryIdentity() throws Exception {
        TaskResultResponse result = TaskResultResponse.newBuilder()
                .setJobId("job-1").setTaskId("task-1").setWorkerId(101)
                .setSuccess(true).setResultValue(0).setAttempt(2).setCoordinatorTerm(7)
                .build();
        TaskProgressUpdate terminal = TaskProgressUpdate.newBuilder()
                .setJobId(result.getJobId()).setTaskId(result.getTaskId()).setWorkerId(101)
                .setUnitsCompleted(4).setTotalUnits(4).setStatusMessage("COMPLETED")
                .setAttempt(2).setCoordinatorTerm(7).setFinalResult(result)
                .build();

        DynamicMessage legacy = DynamicMessage.parseFrom(legacyProgressDescriptor(), terminal.toByteArray());
        assertEquals("task-1", legacy.getField(legacy.getDescriptorForType().findFieldByNumber(2)));
        assertEquals(4L, legacy.getField(legacy.getDescriptorForType().findFieldByNumber(5)));
        assertTrue(legacy.getUnknownFields().hasField(9));
        assertTrue(legacy.getUnknownFields().hasField(10));
        assertTrue(legacy.getUnknownFields().hasField(11));

        TaskProgressUpdate restored = TaskProgressUpdate.parseFrom(legacy.toByteArray());
        assertTrue(restored.hasFinalResult());
        assertEquals(result, restored.getFinalResult());
        assertEquals(2, restored.getAttempt());
        assertEquals(7, restored.getCoordinatorTerm());
    }

    @Test
    public void legacyDispatchStillExecutesWithoutRetryMetadata() {
        TaskDispatchRequest request = TaskDispatchRequest.newBuilder()
                .setJobId("legacy-job").setTaskId("legacy-task")
                .setJobType(JobTypeProto.MAX).addDataChunk(-5).addDataChunk(12)
                .build();
        TaskResultResponse result = new GranularJobRunner(request, 101, null).call();

        assertEquals(0, request.getAttempt());
        assertEquals(0, request.getCoordinatorTerm());
        assertTrue(result.getSuccess());
        assertEquals(12L, result.getResultValue());
        assertFalse(result.getCancelled());
        assertEquals(0, result.getAttempt());
        assertEquals(0, result.getCoordinatorTerm());
        assertFalse(TaskProgressUpdate.getDefaultInstance().hasFinalResult());
    }

    @Test
    public void streamCanDistinguishFailureFromCancellation() throws Exception {
        TaskResultResponse failure = TaskResultResponse.newBuilder()
                .setJobId("job-2").setTaskId("task-2").setWorkerId(102)
                .setErrorMessage("Computation failed").setAttempt(3).setCoordinatorTerm(8)
                .build();
        TaskResultResponse cancellation = failure.toBuilder()
                .setCancelled(true).setErrorMessage("Task cancelled").build();

        for (TaskResultResponse result : new TaskResultResponse[] { failure, cancellation }) {
            TaskProgressUpdate update = TaskProgressUpdate.newBuilder()
                    .setJobId(result.getJobId()).setTaskId(result.getTaskId())
                    .setWorkerId(result.getWorkerId()).setAttempt(result.getAttempt())
                    .setCoordinatorTerm(result.getCoordinatorTerm()).setFinalResult(result)
                    .build();
            TaskResultResponse decoded = TaskProgressUpdate.parseFrom(update.toByteArray()).getFinalResult();
            assertFalse(decoded.getSuccess());
            assertEquals(result.getCancelled(), decoded.getCancelled());
            assertEquals(result.getErrorMessage(), decoded.getErrorMessage());
            assertEquals(3, decoded.getAttempt());
            assertEquals(8, decoded.getCoordinatorTerm());
        }
    }

    private static Descriptors.Descriptor legacyProgressDescriptor() throws Exception {
        DescriptorProto current = TaskProgressUpdate.getDescriptor().toProto();
        DescriptorProto.Builder legacy = current.toBuilder().clearField();
        current.getFieldList().stream().filter(field -> field.getNumber() <= 8)
                .forEach(legacy::addField);
        FileDescriptorProto file = WorkerServiceProto.getDescriptor().toProto().toBuilder()
                .clearMessageType().clearEnumType().clearService().addMessageType(legacy)
                .build();
        return Descriptors.FileDescriptor.buildFrom(file, new Descriptors.FileDescriptor[0])
                .findMessageTypeByName("TaskProgressUpdate");
    }
}
