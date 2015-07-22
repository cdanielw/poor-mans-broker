package org.openforis.rmb.messagebroker.spring;

import org.openforis.rmb.messagebroker.KeepAliveMessageHandler;
import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.MessageHandler;
import org.openforis.rmb.messagebroker.spi.ThrottlingStrategy;
import org.springframework.beans.factory.InitializingBean;

import java.util.concurrent.TimeUnit;

public final class SpringMessageConsumer<T> implements InitializingBean {
    private final MessageConsumer.Builder<T> builder;
    private MessageConsumer<T> consumer;

    private int messagesHandledInParallel = 1;
    private Integer retries = null;
    private ThrottlingStrategy throttlingStrategy = new ThrottlingStrategy.ExponentialBackoff(1, TimeUnit.MINUTES);
    private int timeoutSeconds = 600;

    public SpringMessageConsumer(String consumerId, MessageHandler<T> messageHandler) {
        this(consumerId, messageHandler, null);
    }

    public SpringMessageConsumer(String consumerId, KeepAliveMessageHandler<T> keepAliveMessageHandler) {
        this(consumerId, null, keepAliveMessageHandler);
    }

    private SpringMessageConsumer(String consumerId, MessageHandler<T> messageHandler, KeepAliveMessageHandler<T> keepAliveMessageHandler) {
        builder = messageHandler == null
                ? MessageConsumer.builder(consumerId, keepAliveMessageHandler)
                : MessageConsumer.builder(consumerId, messageHandler);


    }

    MessageConsumer<T> getDelegate() {
        return consumer;
    }

    public void setMessagesHandledInParallel(int messagesHandledInParallel) {
        this.messagesHandledInParallel = messagesHandledInParallel;
    }

    public void setRetries(Integer retries) {
        this.retries = retries;
    }

    public void setThrottlingStrategy(ThrottlingStrategy throttlingStrategy) {
        this.throttlingStrategy = throttlingStrategy;
    }

    public void setTimeoutSeconds(int timeoutSeconds) {
        this.timeoutSeconds = timeoutSeconds;
    }

    public void afterPropertiesSet() throws Exception {
        if (messagesHandledInParallel < 1)
            throw new IllegalArgumentException("A consumer must have a messagesHandledInParallel of at least one");
        builder.messagesHandledInParallel(messagesHandledInParallel);

        ThrottlingStrategy actualThrottlingStrategy = throttlingStrategy == null
                ? ThrottlingStrategy.NO_THROTTLING
                : throttlingStrategy;
        if (retries == null)
            builder.retry(ThrottlingStrategy.NO_THROTTLING);
        else if (retries <= 0)
            builder.neverRetry();
        else
            builder.retry(retries, actualThrottlingStrategy);


        if (timeoutSeconds < 1)
            throw new IllegalArgumentException("A consumer must have a timeoutSeconds of at least one");
        builder.timeout(timeoutSeconds, TimeUnit.SECONDS);

        consumer = builder.build();
    }
}
