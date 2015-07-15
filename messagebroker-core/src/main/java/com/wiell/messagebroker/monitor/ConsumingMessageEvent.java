package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageProcessingUpdate;

public abstract class ConsumingMessageEvent<T> implements Event {
    final MessageProcessingUpdate<T> update;
    final T message;

    public ConsumingMessageEvent(MessageProcessingUpdate<T> update, T message) {
        this.update = update;
        this.message = message;
    }
}
