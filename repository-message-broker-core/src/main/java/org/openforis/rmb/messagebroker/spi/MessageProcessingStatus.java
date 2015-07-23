package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.util.Is;

import java.util.Date;

public final class MessageProcessingStatus {
    final State state;
    final int retries;
    final String errorMessage;
    final Date lastUpdated;
    final String versionId;

    public MessageProcessingStatus(State state, int retries, String errorMessage, Date lastUpdated, String versionId) {
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

    public enum State {
        PENDING, PROCESSING, TIMED_OUT, COMPLETED, FAILED
    }
}
