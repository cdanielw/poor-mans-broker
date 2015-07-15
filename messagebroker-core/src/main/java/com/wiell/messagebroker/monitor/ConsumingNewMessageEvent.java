package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public class ConsumingNewMessageEvent<T> extends ConsumingMessageEvent<T> {
    public ConsumingNewMessageEvent(MessageProcessingUpdate<T> update, T message) {
        super(update, message);
    }

    public String toString() {
        return "ConsumingNewMessageEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
