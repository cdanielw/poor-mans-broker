package org.openforis.rmb.messagebroker.spi;

public final class MessageDetails {
    public final String queueId;
    public final String messageId;
    public final long publicationTime;

    public MessageDetails(String queueId, String messageId, long publicationTime) {
        this.queueId = queueId;
        this.messageId = messageId;
        this.publicationTime = publicationTime;
    }
}
