package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public final class MessageConsumedEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;

    public MessageConsumedEvent(MessageProcessingUpdate<?> update, Object message) {
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
