package org.openforis.rmb.messagebroker.spring;

import org.openforis.rmb.messagebroker.MessageQueue;
import org.openforis.rmb.messagebroker.NotInTransaction;

import java.util.List;


public final class SpringMessageQueue<T> implements MessageQueue<T> {
    private final MessageQueue<T> delegate;

    public SpringMessageQueue(SpringMessageBroker springMessageBroker,
                              String queueId,
                              Class<T> messageType,
                              List<SpringMessageConsumer<T>> consumers) {
        Builder<T> builder = springMessageBroker.messageBroker.queueBuilder(queueId, messageType);
        for (SpringMessageConsumer<T> consumer : consumers)
            builder.consumer(consumer.getDelegate());
        delegate = builder.build();
    }

    public void publish(T message) throws NotInTransaction {
        delegate.publish(message);
    }
}
