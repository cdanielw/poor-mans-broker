package com.wiell.messagebroker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public interface RequestResponseMessageQueue<M, R> {
    Future<R> publish(M message);

    public static final class Builder<M, R> {
        private RespondingMessageHandler<M, R> respondingMessageHandler;
        private Publisher publisher;
        private List<ConsumerBuilder<M, R>> consumerBuilders = new ArrayList<ConsumerBuilder<M, R>>();

        Builder(RespondingMessageHandler<M, R> respondingMessageHandler, Publisher publisher) {
            this.respondingMessageHandler = respondingMessageHandler;
            this.publisher = publisher;
        }

        public ConsumerBuilder<M, R> consumer(String consumerId, MessageResponseHandler<M, R> handler) {
            ConsumerBuilder<M, R> consumerBuilder = new ConsumerBuilder<M, R>(this, consumerId, handler);
            consumerBuilders.add(consumerBuilder);
            return consumerBuilder;
        }

        public RequestResponseMessageQueue<M, R> build() {
            return new Default<M, R>(this);
        }

        private List<Consumer<M, R>> consumers() {
            List<Consumer<M, R>> consumers = new ArrayList<Consumer<M, R>>();
            for (ConsumerBuilder<M, R> consumerBuilder : consumerBuilders)
                consumers.add(new Consumer<M, R>(consumerBuilder));
            return consumers;
        }
    }

    public static final class ConsumerBuilder<M, R> {
        private Builder<M, R> queueBuilder;
        private final String consumerId;
        private final MessageResponseHandler<M, R> handler;
        private int time;
        private TimeUnit timeUnit;

        private ConsumerBuilder(Builder<M, R> queueBuilder, String consumerId, MessageResponseHandler<M, R> handler) {
            this.queueBuilder = queueBuilder;
            this.consumerId = consumerId;
            this.handler = handler;
        }

        public ConsumerBuilder<M, R> timeout(int time, TimeUnit timeUnit) {
            this.time = time;
            this.timeUnit = timeUnit;
            return this;
        }

        public ConsumerBuilder<M, R> consumer(String consumerId, MessageResponseHandler<M, R> handler) {
            return queueBuilder.consumer(consumerId, handler);
        }

        public RequestResponseMessageQueue<M, R> build() {
            return queueBuilder.build();
        }
    }

    static class Consumer<M, R> {
        private final String id;
        private final MessageResponseHandler<M, R> handler;
        private final int time;
        private final TimeUnit timeUnit;

        public Consumer(ConsumerBuilder<M, R> builder) {
            id = builder.consumerId;
            handler = builder.handler;
            time = builder.time;
            timeUnit = builder.timeUnit;
        }

        public void handle(MessageResponse<M, R> messageResponse) {
            handler.handle(messageResponse);
        }
    }


    static class Default<M, R> implements RequestResponseMessageQueue<M, R> {
        final RespondingMessageHandler<M, R> respondingMessageHandler;
        final List<Consumer<M, R>> consumers;
        private final Publisher publisher;

        private Default(Builder<M, R> builder) {
            respondingMessageHandler = builder.respondingMessageHandler;
            consumers = builder.consumers();
            publisher = builder.publisher;
        }

        public Future<R> publish(final M message) {
            return publisher.publish(message, this);
        }
    }
}
