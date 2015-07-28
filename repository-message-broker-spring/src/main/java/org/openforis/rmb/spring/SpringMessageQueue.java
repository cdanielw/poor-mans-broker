package org.openforis.rmb.spring;

import org.openforis.rmb.MessageBroker;
import org.openforis.rmb.MessageQueue;

import java.util.List;


public final class SpringMessageQueue<M> implements MessageQueue<M> {
    private final MessageQueue<M> delegate;

    public SpringMessageQueue(MessageBroker messageBroker,
                              String queueId,
                              List<SpringMessageConsumer<M>> consumers) {
        Builder<M> builder = messageBroker.queueBuilder(queueId);
        for (SpringMessageConsumer<M> consumer : consumers)
            builder.consumer(consumer.getDelegate());
        delegate = builder.build();
    }

    public void publish(M message) {
        delegate.publish(message);
    }
}
