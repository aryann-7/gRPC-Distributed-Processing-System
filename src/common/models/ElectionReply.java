package common.models;

import java.io.Serializable;

/**
 * Reply sent back through the election tree.
 *
 * It contains the best candidate found by a worker
 * and the workers below it.
 */
public class ElectionReply implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String messageId;
    private final int term;
    private final int senderId;
    private final int candidateId;
    private final int candidateJac;

    public ElectionReply(
            String messageId,
            int term,
            int senderId,
            int candidateId,
            int candidateJac) {

        this.messageId = messageId;
        this.term = term;
        this.senderId = senderId;
        this.candidateId = candidateId;
        this.candidateJac = candidateJac;
    }

    public String getMessageId() {
        return messageId;
    }

    public int getTerm() {
        return term;
    }

    public int getSenderId() {
        return senderId;
    }

    public int getCandidateId() {
        return candidateId;
    }

    public int getCandidateJac() {
        return candidateJac;
    }
}