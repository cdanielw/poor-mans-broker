package org.openforis.rmb;

import org.openforis.rmb.monitor.Event;
import org.openforis.rmb.monitor.Monitor;
import org.openforis.rmb.objectserialization.ObjectSerializationMessageSerializer;
import org.openforis.rmb.spi.MessageRepository;
import org.openforis.rmb.spi.MessageSerializer;
import org.openforis.rmb.spi.TransactionSynchronizer;
import org.openforis.rmb.util.Is;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public final class MessageBrokerConfig {
    final MessageRepository messageRepository;
    final TransactionSynchronizer transactionSynchronizer;
    final MessageSerializer messageSerializer;
    final Monitors monitors;
    final long repositoryWatcherPollingPeriod;
    final TimeUnit repositoryWatcherPollingTimeUnit;

    private MessageBrokerConfig(Builder builder) {
        this.messageRepository = builder.messageRepository;
        this.transactionSynchronizer = builder.transactionSynchronizer;
        this.messageSerializer = builder.messageSerializer;
        this.monitors = new Monitors(builder.monitors);
        this.repositoryWatcherPollingPeriod = builder.repositoryWatcherPollingPeriod;
        this.repositoryWatcherPollingTimeUnit = builder.repositoryWatcherPollingTimeUnit;
    }

    public String toString() {
        return "MessageBrokerConfig{" +
                "messageRepository=" + messageRepository +
                ", transactionSynchronizer=" + transactionSynchronizer +
                ", messageSerializer=" + messageSerializer +
                ", monitors=" + monitors +
                '}';
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
        private final List<Monitor<Event>> monitors = new ArrayList<Monitor<Event>>();
        private long repositoryWatcherPollingPeriod = 10;
        private TimeUnit repositoryWatcherPollingTimeUnit = TimeUnit.SECONDS;

        private Builder(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
            this.messageRepository = messageRepository;
            this.transactionSynchronizer = transactionSynchronizer;
        }

        public Builder messageSerializer(MessageSerializer messageSerializer) {
            Is.notNull(messageSerializer, "messageSerializer must not be null");
            this.messageSerializer = messageSerializer;
            return this;
        }

        public Builder repositoryWatcherPollingSchedule(long period, TimeUnit timeUnit) {
            Is.greaterThenZero(period, "period must be greater than zero");
            Is.notNull(timeUnit, "timeUnit must not be null");
            this.repositoryWatcherPollingPeriod = period;
            this.repositoryWatcherPollingTimeUnit = timeUnit;
            return this;
        }

        public Builder monitor(Monitor<Event> monitor) {
            Is.notNull(monitor, "monitor must not be null");
            monitors.add(monitor);
            return this;
        }

        public MessageBrokerConfig build() {
            return new MessageBrokerConfig(this);
        }
    }
}
