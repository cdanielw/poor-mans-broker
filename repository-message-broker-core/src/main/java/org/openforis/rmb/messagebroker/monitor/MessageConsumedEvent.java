package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;
import org.openforis.rmb.messagebroker.util.Is;

public final class MessageConsumedEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;

    public MessageConsumedEvent(MessageProcessingUpdate<?> update, Object message) {
        Is.notNull(update, "update must not be null");
        Is.notNull(message, "message must not be null");
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
