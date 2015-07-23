package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.util.Is;

import java.util.Date;

public final class MessageDetails {
    final String queueId;
    final String messageId;
    final Date publicationTime;

    public MessageDetails(String queueId, String messageId, Date publicationTime) {
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
