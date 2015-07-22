package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.util.Is;

public final class MessageDetails {
    public final String queueId;
    public final String messageId;
    public final long publicationTime;

    public MessageDetails(String queueId, String messageId, long publicationTime) {
        this.queueId = queueId;
        this.messageId = messageId;
        this.publicationTime = publicationTime;
        validate();
    }

    private void validate() {
        Is.notNull(queueId, "queueId must not be null");
        Is.notNull(messageId, "messageId must not be null");
    }


    public String toString() {
        return "MessageDetails{" +
                "queueId='" + queueId + '\'' +
                ", messageId='" + messageId + '\'' +
                ", publicationTime=" + publicationTime +
                '}';
    }
}
