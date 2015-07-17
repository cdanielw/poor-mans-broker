package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class MessageUpdateConflictEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;

    public MessageUpdateConflictEvent(MessageProcessingUpdate<?> update, Object message) {
        this.update = update;
        this.message = message;
    }
}
