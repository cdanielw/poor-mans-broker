package org.openforis.rmb;

import org.openforis.rmb.spi.ThrottlingStrategy;
import org.openforis.rmb.util.Is;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

public final class MessageConsumer<M> {
    final String id;
    final int timeout;
    final TimeUnit timeUnit;
    final int messagesHandledInParallel;
    final int maxRetries;
    final ThrottlingStrategy throttlingStrategy;
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
        Is.hasText(consumerId, "consumerId must be specified");
        Is.notNull(messageHandler, "messageHandler must not be null");
        return new Builder<M>(consumerId, messageHandler, null);
    }

    public static <M> Builder<M> builder(String consumerId, KeepAliveMessageHandler<M> handler) {
        Is.hasText(consumerId, "consumerId must be specified");
        Is.notNull(handler, "handler must not be null");
        return new Builder<M>(consumerId, null, handler);
    }

    public String getId() {
        return id;
    }

    public int getTimeout() {
        return timeout;
    }

    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public ThrottlingStrategy getThrottlingStrategy() {
        return throttlingStrategy;
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
            this.consumerId = consumerId.trim();
            this.handler = handler;
            this.keepAliveHandler = keepAliveHandler;
            timeout(1, MINUTES);
            retryUntilSuccess(new ThrottlingStrategy.ExponentialBackoff(1, MINUTES));
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
            Is.greaterThenZero(maxRetries, "maxRetries must be greater than zero");
            Is.notNull(throttlingStrategy, "throttleStrategy must not be null");
            this.maxRetries = maxRetries;
            this.throttlingStrategy = throttlingStrategy;
            return this;
        }

        public Builder<M> retryUntilSuccess(ThrottlingStrategy throttlingStrategy) {
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
