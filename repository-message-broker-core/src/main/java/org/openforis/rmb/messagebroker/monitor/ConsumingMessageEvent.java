package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageProcessingUpdate;

public abstract class ConsumingMessageEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;

    public ConsumingMessageEvent(MessageProcessingUpdate<?> update, Object message) {
        this.update = update;
        this.message = message;
    }
}
