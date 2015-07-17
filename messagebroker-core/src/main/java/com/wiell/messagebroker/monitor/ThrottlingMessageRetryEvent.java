package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class ThrottlingMessageRetryEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;
    public final Exception exception;

    public ThrottlingMessageRetryEvent(MessageProcessingUpdate<?> update, Object message, Exception exception) {
        this.update = update;
        this.message = message;
        this.exception = exception;
    }
}
