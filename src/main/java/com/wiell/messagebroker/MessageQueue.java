package com.wiell.messagebroker;


import java.util.ArrayList;
import java.util.List;

public interface MessageQueue<M> {
    void publish(M message);

    public static final class Builder<M> {
        private String queueId;
        private MessageQueueManager queueManager;
        private List<MessageConsumer<M>> consumers = new ArrayList<MessageConsumer<M>>();

        @SuppressWarnings("UnusedParameters")
        Builder(String queueId, Class<M> messageType, MessageQueueManager queueManager) {
            this.queueId = queueId;
            this.queueManager = queueManager;
        }

        public Builder<M> add(MessageConsumer.Builder<M> consumer) {
            consumers.add(consumer.build());
            return this;
        }

        public Builder<M> add(MessageConsumer<M> consumer) {
            consumers.add(consumer);
            return this;
        }

        public MessageQueue<M> build() {
            Default<M> queue = new Default<M>(this);
            queueManager.registerQueue(queue);
            return queue;
        }

        private static class Default<M> implements com.wiell.messagebroker.MessageQueue<M> {
            private final String id;
            private final List<MessageConsumer<M>> consumers;
            private final MessageQueueManager queueManager;

            private Default(Builder<M> builder) {
                id = builder.queueId;
                consumers = builder.consumers;
                queueManager = builder.queueManager;
            }

            public void publish(M message) {
                queueManager.publish(id, message, consumers);
            }
        }
    }
}
