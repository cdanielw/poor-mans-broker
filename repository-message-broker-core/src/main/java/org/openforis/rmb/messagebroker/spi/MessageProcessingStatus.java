package org.openforis.rmb.messagebroker.spi;

import java.util.UUID;

public final class MessageProcessingStatus {
    public final MessageProcessingUpdate.Status status;
    public final int retries;
    public final String errorMessage;
    public final String versionId;

    public MessageProcessingStatus(MessageProcessingUpdate.Status status, int retries, String errorMessage) {
        this(status, retries, errorMessage, newVersionId());
    }

    public MessageProcessingStatus(MessageProcessingUpdate.Status status, int retries, String errorMessage, String versionId) {
        this.status = status;
        this.retries = retries;
        this.errorMessage = errorMessage;
        this.versionId = versionId;
    }

    private static String newVersionId() {
        return UUID.randomUUID().toString();
    }
}
