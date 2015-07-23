package org.openforis.rmb.monitor;

import org.openforis.rmb.spi.MessageProcessingUpdate;

public final class ConsumingTimedOutMessageEvent extends ConsumingMessageEvent {
    public ConsumingTimedOutMessageEvent(MessageProcessingUpdate<?> update, Object message) {
        super(update, message);
    }

    public String toString() {
        return "ConsumingTimedOutMessageEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
