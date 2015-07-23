package org.openforis.rmb.monitor;

import org.openforis.rmb.spi.MessageProcessingUpdate;

public final class ConsumingNewMessageEvent extends ConsumingMessageEvent {
    public ConsumingNewMessageEvent(MessageProcessingUpdate<?> update, Object message) {
        super(update, message);
    }

    public String toString() {
        return "ConsumingNewMessageEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
