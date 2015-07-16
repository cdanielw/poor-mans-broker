package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public final class MessageConsumptionFailedEvent<T> implements Event {
    public final MessageProcessingUpdate<T> update;
    public final T message;
    public final Exception e;

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
