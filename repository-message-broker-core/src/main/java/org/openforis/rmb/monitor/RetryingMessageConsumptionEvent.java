package org.openforis.rmb.monitor;

import org.openforis.rmb.spi.MessageProcessingUpdate;
import org.openforis.rmb.util.Is;

public class RetryingMessageConsumptionEvent extends ConsumingMessageEvent {
    public final Exception exception;

    public RetryingMessageConsumptionEvent(MessageProcessingUpdate<?> update, Object message, Exception exception) {
        super(update, message);
        Is.notNull(exception, "exception must not be null");
        this.exception = exception;
    }

    public String toString() {
        return "RetryingMessageConsumptionEvent{" +
                "update=" + update +
                ", message=" + message +
                ", exception=" + exception +
                '}';
    }
}
