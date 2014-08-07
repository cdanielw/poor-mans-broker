package com.wiell.messagebroker;


import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface MessageQueue<M> {
    void publish(M message);

    public static final class Builder<M> {
        private Class<M> messageType;
        private Publisher publisher;
        private List<ConsumerBuilder<M>> consumerBuilders = new ArrayList<ConsumerBuilder<M>>();

        Builder(Class<M> messageType, Publisher publisher) {
            this.messageType = messageType;
            this.publisher = publisher;
        }

        public ConsumerBuilder<M> consumer(String consumerId, MessageHandler<M> handler) {
            ConsumerBuilder<M> consumerBuilder = new ConsumerBuilder<M>(this, consumerId, handler);
            consumerBuilders.add(consumerBuilder);
            return consumerBuilder;
        }

        public MessageQueue<M> build() {
            return new Default<M>(this);
        }

        private List<Consumer<M>> consumers() {
            List<Consumer<M>> consumers = new ArrayList<Consumer<M>>();
            for (ConsumerBuilder<M> consumerBuilder : consumerBuilders)
                consumers.add(new Consumer<M>(consumerBuilder));
            return consumers;
        }
    }

    public static final class ConsumerBuilder<M> {
        private Builder<M> queueBuilder;
        private final String consumerId;
        private final MessageHandler<M> handler;
        private int time;
        private TimeUnit timeUnit;

        private ConsumerBuilder(Builder<M> queueBuilder, String consumerId, MessageHandler<M> handler) {
            this.queueBuilder = queueBuilder;
            this.consumerId = consumerId;
            this.handler = handler;
        }

        public ConsumerBuilder<M> timeout(int time, TimeUnit timeUnit) {
            this.time = time;
            this.timeUnit = timeUnit;
            return this;
        }

        public ConsumerBuilder<M> consumer(String consumerId, MessageHandler<M> handler) {
            return queueBuilder.consumer(consumerId, handler);
        }

        public MessageQueue<M> build() {
            return queueBuilder.build();
        }
    }

    static class Consumer<M> {
        private final String id;
        private final MessageHandler<M> handler;
        private final int time;
        private final TimeUnit timeUnit;

        public Consumer(ConsumerBuilder<M> builder) {
            id = builder.consumerId;
            handler = builder.handler;
            time = builder.time;
            timeUnit = builder.timeUnit;
        }

        public void handle(M message) {
            handler.handle(message);
        }
    }

    static class Default<M> implements com.wiell.messagebroker.MessageQueue<M> {
        final Class<M> messageType;
        final List<Consumer<M>> consumers;
        private final Publisher publisher;

        private Default(Builder<M> builder) {
            messageType = builder.messageType;
            consumers = builder.consumers();
            publisher = builder.publisher;
        }

        public void publish(M message) {
            publisher.publish(message, this);
        }
    }
}
