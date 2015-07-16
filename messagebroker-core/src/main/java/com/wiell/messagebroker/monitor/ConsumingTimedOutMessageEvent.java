package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public final class ConsumingTimedOutMessageEvent<T> extends ConsumingMessageEvent<T> {
    public ConsumingTimedOutMessageEvent(MessageProcessingUpdate<T> update, T message) {
        super(update, message);
    }

    public String toString() {
        return "ConsumingTimedOutMessageEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
