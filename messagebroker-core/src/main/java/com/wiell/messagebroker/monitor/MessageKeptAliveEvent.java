package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class MessageKeptAliveEvent<T> implements Event {
    private final MessageProcessingUpdate<T> update;
    private final T message;

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
