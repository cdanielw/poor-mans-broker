package com.wiell.messagebroker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public interface RequestResponseMessageQueue<M, R> {
    Future<R> publish(M message);

    public static final class Builder<M, R> {
        private String queueId;
        private RespondingMessageHandler<M, R> respondingMessageHandler;
        private final MessageRepository messageRepository;
        private final MessageSerializer messageSerializer;
        private List<MessageConsumer<MessageResponse<M, R>>> consumers = new ArrayList<MessageConsumer<MessageResponse<M, R>>>();

        Builder(String queueId, RespondingMessageHandler<M, R> respondingMessageHandler, MessageRepository messageRepository, MessageSerializer messageSerializer) {
            this.queueId = queueId;
            this.respondingMessageHandler = respondingMessageHandler;
            this.messageRepository = messageRepository;
            this.messageSerializer = messageSerializer;
        }

        public Builder<M, R> add(MessageConsumer.Builder<MessageResponse<M, R>> consumer) {
            consumers.add(consumer.build());
            return this;
        }

        public Builder<M, R> add(MessageConsumer<MessageResponse<M, R>> consumer) {
            consumers.add(consumer);
            return this;
        }

        public RequestResponseMessageQueue<M, R> build() {
            return new Default<M, R>(this);
        }

        private static class Default<M, R> implements RequestResponseMessageQueue<M, R> {
            private final String id;
            private final RespondingMessageHandler<M, R> respondingMessageHandler;
            private final List<MessageConsumer<MessageResponse<M, R>>> consumers;
            private final MessageRepository messageRepository;
            private final MessageSerializer messageSerializer;

            private Default(Builder<M, R> builder) {
                id = builder.queueId;
                respondingMessageHandler = builder.respondingMessageHandler;
                consumers = builder.consumers;
                messageRepository = builder.messageRepository;
                messageSerializer = builder.messageSerializer;
            }

            public Future<R> publish(final M message) {
                // TODO: Implement...
                return null;
            }
        }
    }
}
