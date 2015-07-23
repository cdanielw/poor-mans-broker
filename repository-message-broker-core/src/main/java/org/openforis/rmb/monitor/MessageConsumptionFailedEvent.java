package org.openforis.rmb.monitor;

import org.openforis.rmb.spi.MessageProcessingUpdate;
import org.openforis.rmb.util.Is;

public final class MessageConsumptionFailedEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;
    public final Exception e;

    public MessageConsumptionFailedEvent(MessageProcessingUpdate<?> update, Object message, Exception e) {
        Is.notNull(update, "update must not be null");
        Is.notNull(message, "message must not be null");
        Is.notNull(e, "exception must not be null");
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
