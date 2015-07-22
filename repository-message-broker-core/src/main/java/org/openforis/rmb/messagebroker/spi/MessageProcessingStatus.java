package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.util.Is;

import java.util.UUID;

public final class MessageProcessingStatus {
    public final State state;
    public final int retries;
    public final String errorMessage;
    public final String versionId;

    public MessageProcessingStatus(State state, int retries, String errorMessage) {
        this(state, retries, errorMessage, newVersionId());
    }

    public MessageProcessingStatus(State state, int retries, String errorMessage, String versionId) {
        this.state = state;
        this.retries = retries;
        this.errorMessage = errorMessage;
        this.versionId = versionId;
        validate();
    }

    private static String newVersionId() {
        return UUID.randomUUID().toString();
    }

    public String toString() {
        return "MessageProcessingStatus{" +
                "state=" + state +
                ", retries=" + retries +
                ", errorMessage='" + errorMessage + '\'' +
                ", versionId='" + versionId + '\'' +
                '}';
    }
    private void validate() {
        Is.notNull(state, "state must not be null");
        Is.zeroOrGreater(retries, "retries cannot be negative");
        Is.notNull(versionId, "versionId must not be null");
    }

    public enum State {
        PENDING, PROCESSING, TIMED_OUT, COMPLETED, FAILED
    }
}
