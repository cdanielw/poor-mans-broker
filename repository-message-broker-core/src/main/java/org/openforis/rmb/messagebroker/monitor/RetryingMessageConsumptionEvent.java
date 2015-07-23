package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;
import org.openforis.rmb.messagebroker.util.Is;

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
