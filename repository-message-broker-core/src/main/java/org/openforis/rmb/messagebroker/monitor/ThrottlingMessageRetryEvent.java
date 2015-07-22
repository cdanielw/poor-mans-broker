package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;

public class ThrottlingMessageRetryEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;
    public final Exception exception;

    public ThrottlingMessageRetryEvent(MessageProcessingUpdate<?> update, Object message, Exception exception) {
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
