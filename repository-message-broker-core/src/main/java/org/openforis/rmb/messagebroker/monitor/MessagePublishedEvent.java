package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.util.Is;

public final class MessagePublishedEvent implements Event {
    public final String queueId;
    public final Object message;

    public MessagePublishedEvent(String queueId, Object message) {
        Is.notNull(queueId, "queueId must not be null");
        Is.notNull(message, "message must not be null");
        this.queueId = queueId;
        this.message = message;
    }

    public String toString() {
        return "MessagePublishedEvent{" +
                "queueId='" + queueId + '\'' +
                ", message=" + message +
                '}';
    }
}
