package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class RetryingMessageConsumptionEvent<T> extends ConsumingMessageEvent<T> {
    private final Exception exception;

    public RetryingMessageConsumptionEvent(MessageProcessingUpdate<T> update, T message, Exception exception) {
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
