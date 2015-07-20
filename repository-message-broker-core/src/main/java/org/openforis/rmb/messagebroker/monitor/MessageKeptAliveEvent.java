package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageProcessingUpdate;

public final class MessageKeptAliveEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;

    public MessageKeptAliveEvent(MessageProcessingUpdate<?> update, Object message) {
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
