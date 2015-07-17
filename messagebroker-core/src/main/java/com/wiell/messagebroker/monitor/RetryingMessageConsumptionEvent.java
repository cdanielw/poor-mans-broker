package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class RetryingMessageConsumptionEvent extends ConsumingMessageEvent {
    public final Exception exception;

    public RetryingMessageConsumptionEvent(MessageProcessingUpdate<?> update, Object message, Exception exception) {
        super(update, message);
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
