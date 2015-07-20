package org.openforis.rmb.messagebroker.monitor;

public final class MessagePublishedEvent implements Event {
    public final String queueId;
    public final Object message;

    public MessagePublishedEvent(String queueId, Object message) {
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
