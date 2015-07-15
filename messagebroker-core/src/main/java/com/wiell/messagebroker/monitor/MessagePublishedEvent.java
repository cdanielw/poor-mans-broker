package com.wiell.messagebroker.monitor;

public class MessagePublishedEvent<M> implements Event {
    final String queueId;
    final M message;

    public MessagePublishedEvent(String queueId, M message) {
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
