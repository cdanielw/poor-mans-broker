package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;
import org.openforis.rmb.messagebroker.util.Is;

public class ThrottlingMessageRetryEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;
    public final Exception exception;

    public ThrottlingMessageRetryEvent(MessageProcessingUpdate<?> update, Object message, Exception exception) {
        Is.notNull(update, "update must not be null");
        Is.notNull(message, "message must not be null");
        Is.notNull(exception, "exception must not be null");
        this.update = update;
        this.message = message;
        this.exception = exception;
    }

    public String toString() {
        return "ThrottlingMessageRetryEvent{" +
                "update=" + update +
                ", message=" + message +
                ", exception=" + exception +
                '}';
    }
}
