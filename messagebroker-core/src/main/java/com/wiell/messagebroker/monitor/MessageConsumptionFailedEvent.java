package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public final class MessageConsumptionFailedEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;
    public final Exception e;

    public MessageConsumptionFailedEvent(MessageProcessingUpdate<?> update, Object message, Exception e) {
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
