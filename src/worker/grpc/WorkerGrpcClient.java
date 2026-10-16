package worker.grpc;

import common.grpc.*;
import common.interfaces.WorkerService;
import common.models.ElectionMessage;
import common.models.ElectionReply;
import common.models.JobResult;
import common.models.JobTask;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.IntFunction;

// Keeps the existing scheduler/election interfaces local; all remote calls use gRPC.
public final class WorkerGrpcClient implements WorkerService, AutoCloseable {
    private final WorkerDescriptor descriptor;
    private final PeerGrpcClient peer;
    private final IntFunction<WorkerIdentity> identity;

    public WorkerGrpcClient(WorkerDescriptor descriptor, IntFunction<WorkerIdentity> identity) {
        this.descriptor = descriptor;
        this.identity = identity;
        peer = new PeerGrpcClient(descriptor.getEndpoint().getHost(), descriptor.getEndpoint().getPort());
    }

    public WorkerDescriptor descriptor() { return descriptor; }

    @Override
    public void addNeighbour(int id, WorkerService worker) {
        if (!(worker instanceof WorkerGrpcClient client) || client.descriptor.getIdentity().getWorkerId() != id)
            throw new IllegalArgumentException("Neighbour must supply a matching typed gRPC descriptor");
        if (!peer.addNeighbour(client.descriptor).getNeighbour().equals(client.descriptor))
            throw new IllegalStateException("Peer registered a different session");
    }

    @Override
    public void receiveElectionMessage(ElectionMessage message) {
        var stub = ElectionServiceGrpc.newBlockingStub(peer.channel()).withDeadlineAfter(10, TimeUnit.SECONDS);
        if (message.getType() == ElectionMessage.MessageType.COORDINATOR) {
            stub.announceCoordinator(CoordinatorAnnouncement.newBuilder().setMessageId(message.getMessageId())
                    .setTerm(message.getTerm()).setSenderId(message.getSenderId())
                    .setCoordinator(identity.apply(message.getCandidateId())).setCoordinatorJac(message.getCandidateJac()).build());
        } else {
            stub.receiveElection(ElectionRequest.newBuilder().setElectionId(message.getMessageId())
                    .setTerm(message.getTerm()).setSenderId(message.getSenderId())
                    .setCandidateId(message.getCandidateId()).setCandidateJac(message.getCandidateJac()).build());
        }
    }

    @Override
    public void receiveElectionReply(ElectionReply reply) {
        ElectionServiceGrpc.newBlockingStub(peer.channel()).withDeadlineAfter(10, TimeUnit.SECONDS)
                .receiveElectionReply(ElectionReplyRequest.newBuilder().setElectionId(reply.getMessageId())
                        .setTerm(reply.getTerm()).setSenderId(reply.getSenderId())
                        .setCandidateId(reply.getCandidateId()).setCandidateJac(reply.getCandidateJac()).build());
    }

    @Override
    public boolean ping() {
        return WorkerServiceGrpc.newBlockingStub(peer.channel()).withDeadlineAfter(5, TimeUnit.SECONDS)
                .ping(HeartbeatRequest.getDefaultInstance()).getWorkerId() == descriptor.getIdentity().getWorkerId();
    }

    @Override
    public JobResult executeJob(JobTask task) { return executeJob(task, update -> {}); }

    public JobResult executeJob(JobTask task, Consumer<TaskProgressUpdate> progress) {
        var request = TaskDispatchRequest.newBuilder().setJobId(task.getJobId()).setTaskId(task.getTaskId())
                .setJobType(JobTypeProto.valueOf(task.getJobType().name()))
                .setRangeStart(task.getRangeStart()).setRangeEnd(task.getRangeEnd());
        if (task.getDataChunk() != null) request.addAllDataChunk(task.getDataChunk());
        var updates = WorkerServiceGrpc.newBlockingStub(peer.channel()).withDeadlineAfter(5, TimeUnit.MINUTES)
                .executeTaskStream(request.build());
        TaskResultResponse result = null;
        while (updates.hasNext()) {
            TaskProgressUpdate update = updates.next();
            progress.accept(update);
            if (update.hasFinalResult()) result = update.getFinalResult();
        }
        if (result == null) throw new IllegalStateException("Worker stream ended without a task result");
        return new JobResult(result.getJobId(), result.getTaskId(), result.getSuccess(), result.getResultValue(),
                result.getErrorMessage(), result.getExecutionTimeMs());
    }

    @Override
    public void close() { peer.close(); }
}
