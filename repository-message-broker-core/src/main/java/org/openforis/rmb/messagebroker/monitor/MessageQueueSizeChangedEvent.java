package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.util.Is;

public class MessageQueueSizeChangedEvent implements Event {
    public final String queueId;
    public final MessageConsumer<?> consumer;
    public final int size;

    public MessageQueueSizeChangedEvent(String queueId, MessageConsumer<?> consumer, int size) {
        Is.notNull(queueId, "queueId must not be null");
        Is.notNull(consumer, "consumer must not be null");
        Is.zeroOrGreater(size, "size must be zero or greater");
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
