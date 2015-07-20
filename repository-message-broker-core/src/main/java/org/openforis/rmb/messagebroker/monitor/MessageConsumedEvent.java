package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageProcessingUpdate;

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
