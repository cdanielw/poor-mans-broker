package org.openforis.rmb.spring;

import org.openforis.rmb.MessageBroker;
import org.openforis.rmb.MessageQueue;

import java.util.List;


public final class SpringMessageQueue<T> implements MessageQueue<T> {
    private final MessageQueue<T> delegate;

    public SpringMessageQueue(MessageBroker messageBroker,
                              String queueId,
                              List<SpringMessageConsumer<T>> consumers) {
        Builder<T> builder = messageBroker.queueBuilder(queueId);
        for (SpringMessageConsumer<T> consumer : consumers)
            builder.consumer(consumer.getDelegate());
        delegate = builder.build();
    }

    public void publish(T message) {
        delegate.publish(message);
    }
}
