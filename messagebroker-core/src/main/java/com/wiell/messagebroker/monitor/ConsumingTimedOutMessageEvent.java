package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

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
