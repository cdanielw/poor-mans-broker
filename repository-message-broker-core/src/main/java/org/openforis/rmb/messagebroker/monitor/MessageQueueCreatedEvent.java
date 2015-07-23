package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.util.Is;

import java.util.List;

public final class MessageQueueCreatedEvent implements Event {
    public final String queueId;
    public final List<MessageConsumer<?>> consumers;

    public MessageQueueCreatedEvent(String queueId, List<MessageConsumer<?>> consumers) {
        Is.notNull(queueId, "queueId must not be null");
        Is.notEmpty(consumers, "consumers must not be empty");
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
