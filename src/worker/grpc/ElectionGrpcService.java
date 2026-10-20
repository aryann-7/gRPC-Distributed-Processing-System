package worker.grpc;

import common.grpc.CoordinatorAnnouncement;
import common.grpc.ElectionAcknowledgement;
import common.grpc.ElectionReplyRequest;
import common.grpc.ElectionRequest;
import common.grpc.ElectionServiceGrpc;
import common.models.ElectionMessage;
import common.models.ElectionReply;
import io.grpc.stub.StreamObserver;
import java.util.function.Consumer;

/**
 * Server-side gRPC implementation of distrilab.ElectionService.
 * Handles incoming ReceiveElection, ReceiveElectionReply, and AnnounceCoordinator calls.
 */
public final class ElectionGrpcService extends ElectionServiceGrpc.ElectionServiceImplBase {

    private final Consumer<ElectionMessage> electionMessageConsumer;
    private final Consumer<ElectionReply> electionReplyConsumer;

    public ElectionGrpcService(Consumer<ElectionMessage> electionMessageConsumer,
                               Consumer<ElectionReply> electionReplyConsumer) {
        this.electionMessageConsumer = electionMessageConsumer;
        this.electionReplyConsumer = electionReplyConsumer;
    }

    @Override
    public void receiveElection(ElectionRequest request, StreamObserver<ElectionAcknowledgement> observer) {
        try {
            ElectionMessage msg = new ElectionMessage(
                    request.getElectionId(),
                    ElectionMessage.MessageType.ELECTION,
                    request.getCandidateId(),
                    request.getCandidateJac(),
                    request.getTerm(),
                    request.getSenderId()
            );
            if (electionMessageConsumer != null) {
                electionMessageConsumer.accept(msg);
            }
            observer.onNext(ElectionAcknowledgement.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void receiveElectionReply(ElectionReplyRequest request, StreamObserver<ElectionAcknowledgement> observer) {
        try {
            ElectionReply reply = new ElectionReply(
                    request.getElectionId(),
                    request.getTerm(),
                    request.getSenderId(),
                    request.getCandidateId(),
                    request.getCandidateJac()
            );
            if (electionReplyConsumer != null) {
                electionReplyConsumer.accept(reply);
            }
            observer.onNext(ElectionAcknowledgement.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }

    @Override
    public void announceCoordinator(CoordinatorAnnouncement request, StreamObserver<ElectionAcknowledgement> observer) {
        try {
            int coordId = request.getCoordinator().getWorkerId();
            ElectionMessage msg = new ElectionMessage(
                    request.getMessageId(),
                    ElectionMessage.MessageType.COORDINATOR,
                    coordId,
                    request.getCoordinatorJac(),
                    request.getTerm(),
                    request.getSenderId()
            );
            if (electionMessageConsumer != null) {
                electionMessageConsumer.accept(msg);
            }
            observer.onNext(ElectionAcknowledgement.getDefaultInstance());
            observer.onCompleted();
        } catch (Exception e) {
            observer.onError(io.grpc.Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
        }
    }
}
