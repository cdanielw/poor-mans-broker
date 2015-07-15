package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class MessageConsumptionFailedEvent<T> implements Event {
    private final MessageProcessingUpdate<T> update;
    private final T message;
    private final Exception e;

    public MessageConsumptionFailedEvent(MessageProcessingUpdate<T> update, T message, Exception e) {
        this.update = update;
        this.message = message;
        this.e = e;
    }

    public String toString() {
        return "MessageConsumptionFailedEvent{" +
                "update=" + update +
                ", message=" + message +
                ", e=" + e +
                '}';
    }
}
