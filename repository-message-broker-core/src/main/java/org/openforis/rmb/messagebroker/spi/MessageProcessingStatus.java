package org.openforis.rmb.messagebroker.spi;

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
    }

    private static String newVersionId() {
        return UUID.randomUUID().toString();
    }

    public enum State {
        PENDING, PROCESSING, TIMED_OUT, COMPLETED, FAILED
    }
}
