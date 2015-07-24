package org.openforis.rmb;


import org.openforis.rmb.monitor.Event;
import org.openforis.rmb.monitor.MessageBrokerStartedEvent;
import org.openforis.rmb.monitor.MessageBrokerStoppedEvent;
import org.openforis.rmb.monitor.Monitor;
import org.openforis.rmb.objectserialization.ObjectSerializationMessageSerializer;
import org.openforis.rmb.spi.MessageRepository;
import org.openforis.rmb.spi.MessageSerializer;
import org.openforis.rmb.spi.TransactionSynchronizer;
import org.openforis.rmb.util.Is;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class RepositoryMessageBroker implements MessageBroker {
    private final Monitors monitors;
    private final MessageQueueManager queueManager;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    private RepositoryMessageBroker(Config config) {
        this.monitors = config.monitors;
        this.queueManager = new MessageQueueManager(config);
    }

    public void start() {
        if (stopped.get())
            throw new IllegalStateException("Message broker has been stopped, and cannot be restarted");
        addShutdownHook();
        queueManager.start();
        monitors.onEvent(new MessageBrokerStartedEvent(this));
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            queueManager.stop();
            monitors.onEvent(new MessageBrokerStoppedEvent(this));
        }
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType) {
        Is.hasText(queueId, "queueId must be specified");
        Is.notNull(messageType, "messageType must not be null");
        return new MessageQueue.Builder<M>(queueId, queueManager);
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId) {
        Is.hasText(queueId, "queueId must be specified");
        return new MessageQueue.Builder<M>(queueId, queueManager);
    }

    public String toString() {
        return "RepositoryMessageBroker{" +
                "monitors=" + monitors +
                ", queueManager=" + queueManager +
                ", stopped=" + stopped +
                '}';
    }

    public static Builder builder(MessageRepository messageRepository,
                                  TransactionSynchronizer transactionSynchronizer) {
        Is.notNull(messageRepository, "messageRepository must not be null");
        Is.notNull(transactionSynchronizer, "transactionSynchronizer must not be null");
        return new Builder(messageRepository, transactionSynchronizer);
    }


    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                stop();
            }
        }));
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

        public RepositoryMessageBroker build() {
            return new RepositoryMessageBroker(
                    new Config(
                            messageRepository,
                            transactionSynchronizer,
                            messageSerializer, new Monitors(monitors),
                            repositoryWatcherPollingPeriod,
                            repositoryWatcherPollingTimeUnit
                    )
            );
        }

        public String toString() {
            return "Builder{" +
                    "messageRepository=" + messageRepository +
                    ", transactionSynchronizer=" + transactionSynchronizer +
                    ", messageSerializer=" + messageSerializer +
                    ", monitors=" + monitors +
                    ", repositoryWatcherPollingPeriod=" + repositoryWatcherPollingPeriod +
                    ", repositoryWatcherPollingTimeUnit=" + repositoryWatcherPollingTimeUnit +
                    '}';
        }
    }

    static final class Config {
        final MessageRepository messageRepository;
        final TransactionSynchronizer transactionSynchronizer;
        final MessageSerializer messageSerializer;
        final Monitors monitors;
        final long repositoryWatcherPollingPeriod;
        final TimeUnit repositoryWatcherPollingTimeUnit;

        public Config(
                MessageRepository messageRepository,
                TransactionSynchronizer transactionSynchronizer,
                MessageSerializer messageSerializer,
                Monitors monitors,
                long repositoryWatcherPollingPeriod,
                TimeUnit repositoryWatcherPollingTimeUnit
        ) {
            this.messageRepository = messageRepository;
            this.transactionSynchronizer = transactionSynchronizer;
            this.messageSerializer = messageSerializer;
            this.monitors = monitors;
            this.repositoryWatcherPollingPeriod = repositoryWatcherPollingPeriod;
            this.repositoryWatcherPollingTimeUnit = repositoryWatcherPollingTimeUnit;
        }

        public String toString() {
            return "Config{" +
                    "messageRepository=" + messageRepository +
                    ", transactionSynchronizer=" + transactionSynchronizer +
                    ", messageSerializer=" + messageSerializer +
                    ", monitors=" + monitors +
                    ", repositoryWatcherPollingPeriod=" + repositoryWatcherPollingPeriod +
                    ", repositoryWatcherPollingTimeUnit=" + repositoryWatcherPollingTimeUnit +
                    '}';
        }
    }

}
