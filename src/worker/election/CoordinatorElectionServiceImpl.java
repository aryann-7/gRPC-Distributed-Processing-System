
package worker.election;

import common.grpc.CoordinatorElectionAnnouncement;
import common.grpc.CoordinatorElectionRequest;
import common.grpc.CoordinatorElectionResponse;
import common.grpc.CoordinatorElectionServiceGrpc;
import common.grpc.CoordinatorStateRequest;
import common.grpc.CoordinatorStateResponse;
import common.grpc.TaskCheckpoint;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import worker.recovery.CoordinatorStateStore;

import java.io.IOException;

/**
 * gRPC endpoints for election and coordinator-state recovery.
 */
public class CoordinatorElectionServiceImpl
        extends CoordinatorElectionServiceGrpc.CoordinatorElectionServiceImplBase {

    private final ElectionManager electionManager;
    private final CoordinatorStateStore stateStore;
    private final int localWorkerId;
    private final int localJac;

    private volatile int currentTerm;
    private volatile int currentCoordinatorId;

    public CoordinatorElectionServiceImpl(
            ElectionManager electionManager,
            CoordinatorStateStore stateStore,
            int localWorkerId,
            int localJac,
            int initialTerm) {

        if (electionManager == null || stateStore == null) {
            throw new IllegalArgumentException(
                    "Election manager and state store are required");
        }

        this.electionManager = electionManager;
        this.stateStore = stateStore;
        this.localWorkerId = localWorkerId;
        this.localJac = localJac;
        this.currentTerm = initialTerm;
        this.currentCoordinatorId = -1;
    }

    @Override
    public void startElection(
            CoordinatorElectionRequest request,
            StreamObserver<CoordinatorElectionResponse> observer) {

        if (request.getTerm() < currentTerm) {
            observer.onNext(CoordinatorElectionResponse.newBuilder()
                    .setAccepted(false)
                    .setBestCandidateId(currentCoordinatorId)
                    .setBestCandidateJac(localJac)
                    .setTerm(currentTerm)
                    .setMessage("Rejected stale election term")
                    .build());
            observer.onCompleted();
            return;
        }

        if (request.getMessageId().isBlank()) {
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription("Election message ID is required")
                    .asRuntimeException());
            return;
        }

        currentTerm = request.getTerm();

        common.models.ElectionMessage incoming =
                new common.models.ElectionMessage(
                        request.getMessageId(),
                        common.models.ElectionMessage.MessageType.ELECTION,
                        request.getCandidateId(),
                        request.getCandidateJac(),
                        request.getTerm(),
                        request.getCandidateId());

        if (electionManager.isDuplicateAndMark(
                request.getMessageId())) {
            observer.onNext(CoordinatorElectionResponse.newBuilder()
                    .setAccepted(false)
                    .setBestCandidateId(localWorkerId)
                    .setBestCandidateJac(localJac)
                    .setTerm(currentTerm)
                    .setMessage("Duplicate election message")
                    .build());
            observer.onCompleted();
            return;
        }

        common.models.ElectionMessage best =
                electionManager.processElectionMessage(
                        incoming, localWorkerId, localJac);

        observer.onNext(CoordinatorElectionResponse.newBuilder()
                .setAccepted(true)
                .setBestCandidateId(
                        best == null ? localWorkerId : best.getCandidateId())
                .setBestCandidateJac(
                        best == null ? localJac : best.getCandidateJac())
                .setTerm(currentTerm)
                .setMessage("Election message processed")
                .build());

        observer.onCompleted();
    }

    @Override
    public void announceCoordinator(
            CoordinatorElectionAnnouncement request,
            StreamObserver<CoordinatorElectionResponse> observer) {

        if (request.getTerm() < currentTerm) {
            observer.onNext(CoordinatorElectionResponse.newBuilder()
                    .setAccepted(false)
                    .setBestCandidateId(currentCoordinatorId)
                    .setBestCandidateJac(localJac)
                    .setTerm(currentTerm)
                    .setMessage("Rejected stale coordinator announcement")
                    .build());
            observer.onCompleted();
            return;
        }

        currentTerm = request.getTerm();
        currentCoordinatorId = request.getCoordinatorId();

        observer.onNext(CoordinatorElectionResponse.newBuilder()
                .setAccepted(true)
                .setBestCandidateId(currentCoordinatorId)
                .setBestCandidateJac(request.getCoordinatorJac())
                .setTerm(currentTerm)
                .setMessage("Coordinator announcement accepted")
                .build());

        observer.onCompleted();
    }

    @Override
    public void getCoordinatorState(
            CoordinatorStateRequest request,
            StreamObserver<CoordinatorStateResponse> observer) {

        if (request.getTerm() < currentTerm) {
            observer.onError(Status.FAILED_PRECONDITION
                    .withDescription("Request uses a stale coordinator term")
                    .asRuntimeException());
            return;
        }

        try {
            CoordinatorStateStore.Snapshot snapshot = stateStore.load();

            CoordinatorStateResponse.Builder response =
                    CoordinatorStateResponse.newBuilder()
                            .setCoordinatorId(localWorkerId)
                            .setTerm(currentTerm);

            if (snapshot != null) {
                for (CoordinatorStateStore.TaskSnapshot task
                        : snapshot.tasks()) {

                    response.addTasks(TaskCheckpoint.newBuilder()
                            .setJobId(task.jobId())
                            .setTaskId(task.taskId())
                            .setStatus(task.status())
                            .setAssignedWorkerId(task.assignedWorkerId())
                            .setHasResult(task.hasResult())
                            .setResultValue(task.resultValue())
                            .build());
                }
            }

            observer.onNext(response.build());
            observer.onCompleted();

        } catch (IOException exception) {
            observer.onError(Status.INTERNAL
                    .withDescription("Unable to load coordinator checkpoint")
                    .withCause(exception)
                    .asRuntimeException());
        }
    }
}
