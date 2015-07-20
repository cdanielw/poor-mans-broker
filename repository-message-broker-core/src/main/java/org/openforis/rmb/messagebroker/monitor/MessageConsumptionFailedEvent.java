package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageProcessingUpdate;

public final class MessageConsumptionFailedEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;
    public final Exception e;

    public MessageConsumptionFailedEvent(MessageProcessingUpdate<?> update, Object message, Exception e) {
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
