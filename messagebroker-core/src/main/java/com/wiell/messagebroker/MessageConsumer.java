package com.wiell.messagebroker;

import com.wiell.messagebroker.util.Is;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

public final class MessageConsumer<M> {
    public final String id;
    public final int timeout;
    public final TimeUnit timeUnit;
    public final int messagesHandledInParallel;
    public final int maxRetries;
    public final ThrottlingStrategy throttlingStrategy;
    private final MessageHandler<M> handler;
    private final KeepAliveMessageHandler<M> keepAliveHandler;

    private MessageConsumer(Builder<M> builder) {
        id = builder.consumerId;
        timeout = builder.time;
        timeUnit = builder.timeUnit;
        messagesHandledInParallel = builder.messagesHandledInParallel;
        handler = builder.handler;
        keepAliveHandler = builder.keepAliveHandler;
        maxRetries = builder.maxRetries;
        this.throttlingStrategy = builder.throttlingStrategy;
    }

    void consume(M message, KeepAlive keepAlive) {
        if (handler == null)
            keepAliveHandler.handle(message, keepAlive);
        else
            handler.handle(message);
    }

    public String toString() {
        return "MessageConsumer(" + id + ")";
    }

    public static <M> Builder<M> builder(String consumerId, MessageHandler<M> messageHandler) {
        Is.haveText(consumerId, "consumerId must be specified");
        Is.notNull(messageHandler, "messageHandler must not be null");
        return new Builder<M>(consumerId, messageHandler, null);
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
        private ThrottlingStrategy throttlingStrategy;
        private int messagesHandledInParallel = 1;
        private int maxRetries;

        private Builder(String consumerId, MessageHandler<M> handler, KeepAliveMessageHandler<M> keepAliveHandler) {
            this.consumerId = consumerId;
            this.handler = handler;
            this.keepAliveHandler = keepAliveHandler;
            timeout(1, MINUTES);
            retry(new ThrottlingStrategy.ExponentialBackoff(1, MINUTES));
        }

        public Builder<M> timeout(int time, TimeUnit timeUnit) {
            Is.greaterThenZero(time, "time must be greater then zero");
            Is.notNull(timeUnit, "timeUnit must not be null");
            this.time = time;
            this.timeUnit = timeUnit;
            return this;
        }

        public Builder<M> messagesHandledInParallel(int messagesHandledInParallel) {
            Is.greaterThenZero(messagesHandledInParallel, "messagesHandledInParallel must be greater then zero");
            this.messagesHandledInParallel = messagesHandledInParallel;
            return this;
        }

        public Builder<M> retry(int maxRetries, ThrottlingStrategy throttlingStrategy) {
            Is.zeroOrGreater(maxRetries, "maxRetries must be zero or greater");
            Is.notNull(throttlingStrategy, "throttleStrategy must not be null");
            this.maxRetries = maxRetries;
            this.throttlingStrategy = throttlingStrategy;
            return this;
        }

        public Builder<M> retry(ThrottlingStrategy throttlingStrategy) {
            Is.notNull(throttlingStrategy, "throttleStrategy must not be null");
            this.maxRetries = -1;
            this.throttlingStrategy = throttlingStrategy;
            return this;
        }

        public Builder<M> neverRetry() {
            this.maxRetries = 0;
            this.throttlingStrategy = ThrottlingStrategy.NO_THROTTLING;
            return this;
        }

        public MessageConsumer<M> build() {
            return new MessageConsumer<M>(this);
        }
    }
}
