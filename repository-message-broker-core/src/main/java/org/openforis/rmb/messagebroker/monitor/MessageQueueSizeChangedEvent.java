package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageConsumer;

public class MessageQueueSizeChangedEvent implements Event {
    public final String queueId;
    public final MessageConsumer<?> consumer;
    public final int size;

    public MessageQueueSizeChangedEvent(String queueId, MessageConsumer<?> consumer, int size) {
        this.queueId = queueId;
        this.consumer = consumer;
        this.size = size;
    }

    public String toString() {
        return "MessageQueueSizeChangedEvent{" +
                "queueId='" + queueId + '\'' +
                ", consumer=" + consumer +
                ", size=" + size +
                '}';
    }
}
