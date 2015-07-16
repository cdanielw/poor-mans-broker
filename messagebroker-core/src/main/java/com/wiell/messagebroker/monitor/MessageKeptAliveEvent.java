package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public final class MessageKeptAliveEvent<T> implements Event {
    public final MessageProcessingUpdate<T> update;
    public final T message;

    public MessageKeptAliveEvent(MessageProcessingUpdate<T> update, T message) {
        this.update = update;
        this.message = message;
    }

    public String toString() {
        return "MessageKeptAliveEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
