package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public final class MessageConsumedEvent<T> implements Event {
    public final MessageProcessingUpdate<T> update;
    public final T message;

    public MessageConsumedEvent(MessageProcessingUpdate<T> update, T message) {
        this.update = update;
        this.message = message;
    }

    public String toString() {
        return "MessageConsumedEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
