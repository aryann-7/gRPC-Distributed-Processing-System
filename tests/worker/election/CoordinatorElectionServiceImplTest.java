
package worker.election;

import common.grpc.CoordinatorElectionAnnouncement;
import common.grpc.CoordinatorElectionRequest;
import common.grpc.CoordinatorElectionResponse;
import common.grpc.CoordinatorStateRequest;
import common.grpc.CoordinatorStateResponse;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import worker.recovery.CoordinatorStateStore;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class CoordinatorElectionServiceImplTest {

    @TempDir
    Path temporaryDirectory;

    private static class ResultObserver<T> implements StreamObserver<T> {
        T value;
        Throwable error;
        boolean completed;

        @Override
        public void onNext(T value) {
            this.value = value;
        }

        @Override
        public void onError(Throwable error) {
            this.error = error;
        }

        @Override
        public void onCompleted() {
            completed = true;
        }
    }

    private CoordinatorElectionServiceImpl createService()
            throws Exception {

        CoordinatorStateStore store =
                new CoordinatorStateStore(
                        temporaryDirectory.resolve("state.bin"));

        store.save(new CoordinatorStateStore.Snapshot(
                4, 1, 0, java.util.List.of()));

        return new CoordinatorElectionServiceImpl(
                new ElectionManager(), store, 4, 0, 1);
    }

    @Test
    void acceptsCoordinatorAnnouncementForCurrentTerm()
            throws Exception {

        CoordinatorElectionServiceImpl service = createService();

        CoordinatorElectionAnnouncement request =
                CoordinatorElectionAnnouncement.newBuilder()
                        .setCoordinatorId(4)
                        .setCoordinatorJac(0)
                        .setTerm(2)
                        .build();

        ResultObserver<CoordinatorElectionResponse> observer =
                new ResultObserver<>();

        service.announceCoordinator(request, observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertTrue(observer.value.getAccepted());
        assertEquals(4, observer.value.getBestCandidateId());
        assertEquals(2, observer.value.getTerm());
    }

    @Test
    void rejectsStaleCoordinatorAnnouncement()
            throws Exception {

        CoordinatorElectionServiceImpl service = createService();

        ResultObserver<CoordinatorElectionResponse> first =
                new ResultObserver<>();

        service.announceCoordinator(
                CoordinatorElectionAnnouncement.newBuilder()
                        .setCoordinatorId(4)
                        .setCoordinatorJac(0)
                        .setTerm(3)
                        .build(),
                first);

        ResultObserver<CoordinatorElectionResponse> stale =
                new ResultObserver<>();

        service.announceCoordinator(
                CoordinatorElectionAnnouncement.newBuilder()
                        .setCoordinatorId(8)
                        .setCoordinatorJac(2)
                        .setTerm(2)
                        .build(),
                stale);

        assertNotNull(stale.value);
        assertFalse(stale.value.getAccepted());
        assertEquals(3, stale.value.getTerm());
    }

    @Test
    void returnsSavedCoordinatorState() throws Exception {
        CoordinatorElectionServiceImpl service = createService();

        ResultObserver<CoordinatorStateResponse> observer =
                new ResultObserver<>();

        service.getCoordinatorState(
                CoordinatorStateRequest.newBuilder()
                        .setRequestingCoordinatorId(7)
                        .setTerm(1)
                        .build(),
                observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertEquals(4, observer.value.getCoordinatorId());
        assertEquals(1, observer.value.getTerm());
    }

    @Test
    void processesAnElectionRequest() throws Exception {
        CoordinatorElectionServiceImpl service = createService();

        ResultObserver<CoordinatorElectionResponse> observer =
                new ResultObserver<>();

        service.startElection(
                CoordinatorElectionRequest.newBuilder()
                        .setCandidateId(6)
                        .setCandidateJac(1)
                        .setTerm(2)
                        .setMessageId("election-test-1")
                        .build(),
                observer);

        assertNull(observer.error);
        assertTrue(observer.completed);
        assertNotNull(observer.value);
        assertTrue(observer.value.getAccepted());
        assertEquals(2, observer.value.getTerm());
    }
}
