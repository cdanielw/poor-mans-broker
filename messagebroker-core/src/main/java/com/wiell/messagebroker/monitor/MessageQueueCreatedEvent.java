package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageConsumer;

import java.util.List;

public class MessageQueueCreatedEvent implements Event {
    final String queueId;
    final List<MessageConsumer<?>> consumers;

    public MessageQueueCreatedEvent(String queueId, List<MessageConsumer<?>> consumers) {
        this.queueId = queueId;
        this.consumers = consumers;
    }

    public String toString() {
        return "QueueCreatedEvent{" +
                "queueId='" + queueId + '\'' +
                ", consumers=" + consumers +
                '}';
    }
}
