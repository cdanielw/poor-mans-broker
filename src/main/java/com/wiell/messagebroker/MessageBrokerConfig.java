package com.wiell.messagebroker;

import java.util.concurrent.TimeUnit;

public class MessageBrokerConfig {
    public final MessageRepository messageRepository;
    public final TransactionSynchronizer transactionSynchronizer;
    public final int pollingDelay;
    public final TimeUnit pollingDelayTimeUnit;

    private MessageBrokerConfig(Builder builder) {
        this.messageRepository = builder.messageRepository;
        this.transactionSynchronizer = builder.transactionSynchronizer;
        this.pollingDelay = builder.pollingDelay;
        this.pollingDelayTimeUnit = builder.timeUnit;
    }

    public static Builder builder(MessageRepository messageRepository,
                                  TransactionSynchronizer transactionSynchronizer) {
        return new Builder(messageRepository, transactionSynchronizer);
    }

    public static final class Builder {
        private final MessageRepository messageRepository;
        private final TransactionSynchronizer transactionSynchronizer;
        private int pollingDelay;
        private TimeUnit timeUnit;

        public Builder(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
            this.messageRepository = messageRepository;
            this.transactionSynchronizer = transactionSynchronizer;
        }

        public Builder pollingDelay(int pollingDelay, TimeUnit timeUnit) {
            this.pollingDelay = pollingDelay;
            this.timeUnit = timeUnit;
            return this;
        }

        public MessageBrokerConfig build() {
            return new MessageBrokerConfig(this);
        }
    }
}
