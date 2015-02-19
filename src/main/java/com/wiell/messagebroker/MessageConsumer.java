package com.wiell.messagebroker;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;

public final class MessageConsumer<M> {
    public final String id;
    public final int time;
    public final TimeUnit timeUnit;
    public final boolean blocking;
    public final int workerCount;
    public final int maxRetries;
    private final MessageHandler<M> handler;
    private final KeepAliveMessageHandler<M> keepAliveHandler;


    private MessageConsumer(Builder<M> builder) {
        id = builder.consumerId;
        time = builder.time;
        timeUnit = builder.timeUnit;
        blocking = builder.blocking;
        workerCount = builder.workerCount;
        handler = builder.handler;
        keepAliveHandler = builder.keepAliveHandler;
        maxRetries = builder.maxRetries;
    }

    void consume(M message, KeepAlive keepAlive) {
        if (handler == null)
            keepAliveHandler.handle(message, keepAlive);
        else
            handler.handle(message);
    }

    public static <M> Builder<M> builder(String consumerId, MessageHandler<M> handler) {
        return new Builder<M>(consumerId, handler, null);
    }

    public static <M> Builder<M> builder(String consumerId, KeepAliveMessageHandler<M> handler) {
        return new Builder<M>(consumerId, null, handler);
    }

    public static final class Builder<M> {
        private final String consumerId;
        private final MessageHandler<M> handler;
        private final KeepAliveMessageHandler<M> keepAliveHandler;
        private int time;
        private TimeUnit timeUnit;
        private boolean blocking;
        private ThrottleStrategy throttleStrategy;
        private int workerCount;
        private int maxRetries;

        private Builder(String consumerId, MessageHandler<M> handler, KeepAliveMessageHandler<M> keepAliveHandler) {
            this.consumerId = consumerId;
            this.handler = handler;
            this.keepAliveHandler = keepAliveHandler;
            timeout(10, SECONDS);
            blocking();
            retry(ThrottleStrategy.ONE_SECOND_PER_RETRY);
        }

        public Builder<M> timeout(int time, TimeUnit timeUnit) {
            this.time = time;
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder<M> blocking() {
            this.blocking = true;
            this.workerCount = 1;
            return this;
        }

        public Builder<M> nonBlocking(int workerCount) {
            this.blocking = false;
            this.workerCount = workerCount;
            return this;
        }

        public Builder<M> retry(int maxRetries, ThrottleStrategy throttleStrategy) {
            this.maxRetries = maxRetries;
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public Builder<M> retry(ThrottleStrategy throttleStrategy) {
            this.maxRetries = -1;
            this.throttleStrategy = throttleStrategy;
            return this;
        }

        public Builder<M> noRetries() {
            this.maxRetries = 0;
            this.throttleStrategy = ThrottleStrategy.NO_THROTTLING;
            return this;
        }

        public MessageConsumer<M> build() {
            return new MessageConsumer<M>(this);
        }
    }
}
