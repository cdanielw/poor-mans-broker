package com.wiell.messagebroker;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public interface MessageQueue<M> {
    void publish(M message);

    public static final class Builder<M> {
        private String queueId;
        private final MessageRepository messageRepository;
        private final MessageSerializer messageSerializer;
        private List<MessageConsumer<M>> consumers = new ArrayList<MessageConsumer<M>>();

        @SuppressWarnings("UnusedParameters")
        Builder(String queueId, Class<M> messageType, MessageRepository messageRepository, MessageSerializer messageSerializer) {
            this.queueId = queueId;
            this.messageRepository = messageRepository;
            this.messageSerializer = messageSerializer;
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
            return new Default<M>(this);
        }

        private static class Default<M> implements com.wiell.messagebroker.MessageQueue<M> {
            private final String id;
            private final List<MessageConsumer<M>> consumers;
            private final MessageRepository messageRepository;
            private final MessageSerializer messageSerializer;

            private Default(Builder<M> builder) {
                id = builder.queueId;
                consumers = builder.consumers;
                messageRepository = builder.messageRepository;
                messageSerializer = builder.messageSerializer;
            }

            public void publish(M message) {
                messageRepository.submit(messageSerializer.serialize(message), id, consumers);
                // TODO: Do the rest...
            }
        }
    }
}
