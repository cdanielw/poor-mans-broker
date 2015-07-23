package org.openforis.rmb.messagebroker;


import org.openforis.rmb.messagebroker.util.Is;

import java.util.ArrayList;
import java.util.List;

public interface MessageQueue<M> {
    void publish(M message) throws NotInTransaction;

    final class Builder<M> {
        private String queueId;
        private MessageQueueManager queueManager;
        private List<MessageConsumer<M>> consumers = new ArrayList<MessageConsumer<M>>();

        @SuppressWarnings("UnusedParameters")
        Builder(String queueId, MessageQueueManager queueManager) {
            this.queueId = queueId;
            this.queueManager = queueManager;
        }

        public Builder<M> consumer(MessageConsumer.Builder<M> consumer) {
            Is.notNull(consumer, "consumer must not be null");
            consumers.add(consumer.build());
            return this;
        }

        public Builder<M> consumer(MessageConsumer<M> consumer) {
            Is.notNull(consumer, "consumer must not be null");
            consumers.add(consumer);
            return this;
        }

        @SuppressWarnings("unchecked")
        public MessageQueue<M> build() {
            Default<M> queue = new Default<M>(this);
            List uncheckedConsumers = consumers;
            queueManager.registerQueue(queueId, uncheckedConsumers);
            return queue;
        }

        private static class Default<M> implements org.openforis.rmb.messagebroker.MessageQueue<M> {
            private final String id;
            private final MessageQueueManager queueManager;

            private Default(Builder<M> builder) {
                id = builder.queueId;
                queueManager = builder.queueManager;
            }

            public void publish(M message) {
                Is.notNull(message, "message must not be null");
                queueManager.publish(id, message);
            }
        }
    }
}
