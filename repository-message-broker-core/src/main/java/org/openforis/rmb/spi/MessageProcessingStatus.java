package org.openforis.rmb.spi;

import org.openforis.rmb.util.Is;

import java.util.Date;

/**
 * Represents the current status of a message to be/being processed by a consumer.
 */
public final class MessageProcessingStatus {
    final State state;
    final int retries;
    final String errorMessage;
    final Date lastUpdated;
    final String versionId;

    /**
     * Creates an instance.
     *
     * @param state        the state of the processing
     * @param retries      the number of times the consumer retried the processing
     * @param errorMessage the last error message from a failed processing attempt
     * @param lastUpdated  the last time the status changed
     * @param versionId    the current version of the processing
     */
    public MessageProcessingStatus(State state, int retries, String errorMessage, Date lastUpdated, String versionId) {
        Is.notNull(state, "state must not be null");
        Is.zeroOrGreater(retries, "retries must be zero or greater");
        Is.notNull(lastUpdated, "lastUpdated must not be null");
        Is.hasText(versionId, "versionId must be specified");
        this.state = state;
        this.retries = retries;
        this.errorMessage = errorMessage;
        this.lastUpdated = lastUpdated;
        this.versionId = versionId;
        validate();
    }

    public String toString() {
        return "MessageProcessingStatus{" +
                "state=" + state +
                ", retries=" + retries +
                ", errorMessage='" + errorMessage + '\'' +
                ", lastUpdated=" + lastUpdated +
                ", versionId='" + versionId + '\'' +
                '}';
    }

    private void validate() {
        Is.notNull(state, "state must not be null");
        Is.zeroOrGreater(retries, "retries cannot be negative");
        Is.notNull(versionId, "versionId must not be null");
    }

    /**
     * Represents the state of a message to be/being processed by a consumer.
     */
    public enum State {
        PENDING, PROCESSING, TIMED_OUT, COMPLETED, FAILED
    }
}
