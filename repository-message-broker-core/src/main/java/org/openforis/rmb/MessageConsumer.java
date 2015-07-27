package org.openforis.rmb;

import org.openforis.rmb.inmemory.InMemoryMessageRepository;
import org.openforis.rmb.jdbc.JdbcMessageRepository;
import org.openforis.rmb.spi.MessageRepository;
import org.openforis.rmb.spi.ThrottlingStrategy;
import org.openforis.rmb.util.Is;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;

/**
 * A consumer of published messages. It specifies:
 * </p>
 * <ul>
 * <li>an id, which must be unique within the message broker.
 * <li>a timeout, after which the message broker considers the message abandoned.
 * <li>the number of messages to be handled in parallel. Whether this is something that can be guaranteed at
 * in a cluster depends on {@link MessageRepository} implementation -
 * {@link JdbcMessageRepository} guarantees it while {@link InMemoryMessageRepository} does not.
 * <li>a strategy for dealing with failure. The number of times to retry before giving up and how to throttle
 * the reties.
 * <li>a message handler, that actually do something with published messages. The message handler comes in two flavors,
 * A simple one {@link MessageHandler} and {@link KeepAliveMessageHandler}, for long running handlers,
 * which allows keep-alive to be sent to the message broker, resetting the timeout.
 * </ul>
 * <p>
 * An effort to prevent duplicate messages is made, but it cannot be guaranteed.
 * Therefore it's strongly suggested to make message handlers idempotent idempotent.
 * </p>
 * <p>
 * Instances of this class are created through a builder: {@link #builder(String, MessageHandler)} or
 * </p>
 * {@link #builder(String, KeepAliveMessageHandler)}.
 *
 * @param <M> the type of messages to consume
 */
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

    /**
     * Provides a builder for creating {@link MessageConsumer}s.
     *
     * @param consumerId     the id of the consumer. Must not be null and must be unique within the message broker.
     * @param messageHandler the message handler. Must not be null.
     * @param <M>            the type of messages to handle
     * @return the consumer builder
     */
    public static <M> Builder<M> builder(String consumerId, MessageHandler<M> messageHandler) {
        Is.hasText(consumerId, "consumerId must be specified");
        Is.notNull(messageHandler, "messageHandler must not be null");
        return new Builder<M>(consumerId, messageHandler, null);
    }

    /**
     * Provides a builder for creating {@link MessageConsumer}s.
     *
     * @param consumerId              the id of the consumer. Must not be null and must be unique within the message broker.
     * @param keepAliveMessageHandler the message handler. Must not be null.
     * @param <M>                     the type of messages to handle
     * @return the consumer builder
     */
    public static <M> Builder<M> builder(String consumerId, KeepAliveMessageHandler<M> keepAliveMessageHandler) {
        Is.hasText(consumerId, "consumerId must be specified");
        Is.notNull(keepAliveMessageHandler, "keepAliveMessageHandler must not be null");
        return new Builder<M>(consumerId, null, keepAliveMessageHandler);
    }

    /**
     * Gets the consumer id.
     *
     * @return the consumer id
     */
    public String getId() {
        return id;
    }

    /**
     * The timeout when handling messages before the message broker considers the message abandoned.
     *
     * @return the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * The time unit the timeout is specified in.
     *
     * @return the time unit
     */
    public TimeUnit getTimeUnit() {
        return timeUnit;
    }

    /**
     * The maximum number of messages to be handled in parallel. Whether this is something that can be guaranteed at
     * in a cluster depends on {@link MessageRepository} implementation -
     * {@link JdbcMessageRepository} guarantees it while {@link InMemoryMessageRepository} does not.
     *
     * @return the maximum number of messages to be handled in parallel.
     */
    public int getMessagesHandledInParallel() {
        return messagesHandledInParallel;
    }

    /**
     * The maximum number of times the handler should retry a message after failing to handle it.
     * If the handling never should be retried it's 0, if it should be retried indefinitely until it succeeds it's -1.
     *
     * @return the maximum number of times the handler should retry a message
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /**
     * The throttling strategy to use when retrying a message the handler failed to handle.
     *
     * @return the throttling strategy
     */
    public ThrottlingStrategy getThrottlingStrategy() {
        return throttlingStrategy;
    }

    /**
     * Builds {@link MessageConsumer} instances. Configure the {@link MessageConsumer} to build through the chainable
     * builder methods, then finally call {@link #build()}.
     * <p>
     * If building the message consumer without configuring the builder, the following configuration is used:
     * </p>
     * <ul>
     * <li>{@code timeout(1, MINUTES);}
     * <li>{@code messagesHandledInParallel(1);}
     * <li>{@code retryUntilSuccess(new ThrottlingStrategy.ExponentialBackoff(1, MINUTES));}
     * </ul>
     * <p>
     * Instances of this class are created though {@link MessageConsumer#builder(String, MessageHandler)} or
     * {@link MessageConsumer#builder(String, KeepAliveMessageHandler)}.
     * </p>
     *
     * @param <M> the type of messages to handle
     */
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

        /**
         * Specify the timeout when handling messages before the message broker considers the message abandoned.
         *
         * @param time     the timeout
         * @param timeUnit the time unit of the timeout
         * @return the builder, so methods can be chained
         */
        public Builder<M> timeout(int time, TimeUnit timeUnit) {
            Is.greaterThenZero(time, "time must be greater then zero");
            Is.notNull(timeUnit, "timeUnit must not be null");
            this.time = time;
            this.timeUnit = timeUnit;
            return this;
        }

        /**
         * Specify the maximum number of messages to be handled in parallel. Whether this is something that can be
         * guaranteed at in a cluster depends on {@link MessageRepository} implementation -
         * {@link JdbcMessageRepository} guarantees it while {@link InMemoryMessageRepository} does not.
         *
         * @param messagesHandledInParallel the maximum number of messages to be handled in parallel
         * @return the builder, so methods can be chained
         */
        public Builder<M> messagesHandledInParallel(int messagesHandledInParallel) {
            Is.greaterThenZero(messagesHandledInParallel, "messagesHandledInParallel must be greater then zero");
            this.messagesHandledInParallel = messagesHandledInParallel;
            return this;
        }

        /**
         * Specify that the consumer should retry a message after failing to handle it.
         *
         * @param maxRetries         the maximum number of retries before giving up
         * @param throttlingStrategy the strategy to calculate the delay between each retry
         * @return the builder, so methods can be chained
         */
        public Builder<M> retry(int maxRetries, ThrottlingStrategy throttlingStrategy) {
            Is.greaterThenZero(maxRetries, "maxRetries must be greater than zero");
            Is.notNull(throttlingStrategy, "throttleStrategy must not be null");
            this.maxRetries = maxRetries;
            this.throttlingStrategy = throttlingStrategy;
            return this;
        }

        /**
         * Specify that the consumer should retry a message after failing to handle it indefinitely,
         * until it succeeds.
         *
         * @param throttlingStrategy the strategy to calculate the delay between each retry
         * @return the builder, so methods can be chained
         */
        public Builder<M> retryUntilSuccess(ThrottlingStrategy throttlingStrategy) {
            Is.notNull(throttlingStrategy, "throttleStrategy must not be null");
            this.maxRetries = -1;
            this.throttlingStrategy = throttlingStrategy;
            return this;
        }

        /**
         * Specify that the consumer never should retry a message after failing to handle it, but give up directly.
         *
         * @return the builder, so methods can be chained
         */
        public Builder<M> neverRetry() {
            this.maxRetries = 0;
            this.throttlingStrategy = ThrottlingStrategy.NO_THROTTLING;
            return this;
        }

        /**
         * Builds the {@link MessageConsumer}, based on how the builder's been configured.
         *
         * @return the message consumer instance
         */
        public MessageConsumer<M> build() {
            return new MessageConsumer<M>(this);
        }
    }
}
