package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.Monitor;
import com.wiell.messagebroker.objectserialization.ObjectSerializationMessageSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class MessageBrokerConfig {
    final MessageRepository messageRepository;
    final TransactionSynchronizer transactionSynchronizer;
    final int abandonedJobsPeriod;
    final TimeUnit abandonedJobsTimeUnit;
    final MessageSerializer messageSerializer;
    final Monitors monitors;

    private MessageBrokerConfig(Builder builder) {
        this.messageRepository = builder.messageRepository;
        this.transactionSynchronizer = builder.transactionSynchronizer;
        this.messageSerializer = builder.messageSerializer;
        this.abandonedJobsPeriod = builder.abandonedJobsPeriod;
        this.abandonedJobsTimeUnit = builder.abandonedJobsTimeUnit;
        this.monitors = new Monitors(builder.monitors);
    }

    public static Builder builder(MessageRepository messageRepository,
                                  TransactionSynchronizer transactionSynchronizer) {
        Is.notNull(messageRepository, "messageRepository must not be null");
        Is.notNull(transactionSynchronizer, "transactionSynchronizer must not be null");
        return new Builder(messageRepository, transactionSynchronizer);
    }

    public static final class Builder {
        private final MessageRepository messageRepository;
        private final TransactionSynchronizer transactionSynchronizer;
        private MessageSerializer messageSerializer = new ObjectSerializationMessageSerializer();
        private int abandonedJobsPeriod;
        private TimeUnit abandonedJobsTimeUnit;
        private final List<Monitor> monitors = new ArrayList<Monitor>();

        private Builder(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
            this.messageRepository = messageRepository;
            this.transactionSynchronizer = transactionSynchronizer;
            abandonedJobsCheckingSchedule(1, TimeUnit.MINUTES);
        }

        public Builder messageSerializer(MessageSerializer messageSerializer) {
            this.messageSerializer = messageSerializer;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            monitors.add(monitor);
            return this;
        }

        public Builder abandonedJobsCheckingSchedule(int period, TimeUnit timeUnit) {
            Is.greaterThenZero(period, "period must be greater then zero");
            Is.notNull(timeUnit, "timeUnit must not be null");
            this.abandonedJobsPeriod = period;
            this.abandonedJobsTimeUnit = timeUnit;
            return this;
        }

        public MessageBrokerConfig build() {
            return new MessageBrokerConfig(this);
        }
    }
}
