package worker.election;

import common.models.ElectionMessage;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages flooding-based leader election state.
 *
 * Election rules:
 * - Lowest Job Allocation Counter (JAC) wins.
 * - If JAC is equal, highest worker ID wins.
 * - Unique message IDs prevent duplicate processing.
 */
public class ElectionManager {

    /**
     * Stores message IDs that have already been processed.
     */
    private final Set<String> seenMessageIds =
            ConcurrentHashMap.newKeySet();

    /**
     * Stores the best election candidate known for each term.
     */
    private final Map<Integer, ElectionMessage> bestCandidates =
            new ConcurrentHashMap<>();

    /**
     * Checks whether a message has already been processed.
     *
     * @param messageId unique message ID
     * @return true if the message is a duplicate
     */
    public boolean isDuplicateAndMark(String messageId) {
        if (messageId == null || messageId.isBlank()) {
            return false;
        }

        return !seenMessageIds.add(messageId);
    }

    /**
     * Creates the first ELECTION message for a new election term.
     */
    public ElectionMessage initiateElection(
            int candidateId,
            int jac,
            int term) {

        ElectionMessage message = new ElectionMessage(
                UUID.randomUUID().toString(),
                ElectionMessage.MessageType.ELECTION,
                candidateId,
                jac,
                term,
                candidateId
        );

        bestCandidates.put(term, message);

        return message;
    }

    /**
     * Compares an incoming candidate with the local worker.
     *
     * Lowest JAC wins.
     * If JAC is equal, highest worker ID wins.
     */
    public ElectionMessage processElectionMessage(
            ElectionMessage message,
            int localId,
            int localJac) {

        if (message == null) {
            return null;
        }

        ElectionMessage currentBest =
                bestCandidates.get(message.getTerm());

        ElectionMessage candidate = message;

        if (isBetter(
                localJac,
                localId,
                candidate.getCandidateJac(),
                candidate.getCandidateId())) {

            candidate = new ElectionMessage(
                    message.getMessageId(),
                    ElectionMessage.MessageType.ELECTION,
                    localId,
                    localJac,
                    message.getTerm(),
                    localId
            );
        }

        if (currentBest == null ||
                isBetter(
                        candidate.getCandidateJac(),
                        candidate.getCandidateId(),
                        currentBest.getCandidateJac(),
                        currentBest.getCandidateId())) {

            bestCandidates.put(message.getTerm(), candidate);
        }

        return candidate;
    }

    /**
     * Creates the COORDINATOR announcement message.
     */
    public ElectionMessage createCoordinatorAnnouncement(
            int electedCoordinatorId,
            int coordinatorJac,
            int term) {

        return new ElectionMessage(
                UUID.randomUUID().toString(),
                ElectionMessage.MessageType.COORDINATOR,
                electedCoordinatorId,
                coordinatorJac,
                term,
                electedCoordinatorId
        );
    }

    /**
     * Returns the best candidate currently known for a term.
     */
    public ElectionMessage getBestCandidate(int term) {
        return bestCandidates.get(term);
    }

    /**
     * Clears election state.
     */
    public void resetElectionState() {
        seenMessageIds.clear();
        bestCandidates.clear();
    }

    /**
     * Clears candidate information for a specific term.
     */
    public void resetForTerm(int term) {
        bestCandidates.remove(term);
    }

    /**
     * Determines which candidate is better.
     */
    private boolean isBetter(
            int jacA,
            int idA,
            int jacB,
            int idB) {

        return jacA < jacB ||
                (jacA == jacB && idA > idB);
    }
}