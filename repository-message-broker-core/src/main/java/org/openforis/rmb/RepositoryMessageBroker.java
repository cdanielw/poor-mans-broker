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

/**
 * A {@link MessageBroker} backed by a repository. It is responsible for the creation and management
 * of {@link MessageQueue}s.
 * <p/>
 * Instances are created through a builder: {@link #builder(MessageRepository, TransactionSynchronizer)}.
 */
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

    /**
     * Creates a builder.
     *
     * @param messageRepository       the repository where to store published messages.
     * @param transactionSynchronizer an implementation that allows {@link MessageQueue}s to ensure messages are
     *                                published within transactions and get notified once the transaction commits.
     * @return the builder.
     */
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

    /**
     * Builds {@link RepositoryMessageBroker} instances. Instances of this class are created through
     * {@link RepositoryMessageBroker#builder(MessageRepository, TransactionSynchronizer)}.
     * Configure the {@link RepositoryMessageBroker} to build through the chainable builder methods, then finally call
     * {@link #build()}.
     * <p>
     * If building the message broker without configuring the builder, the following configuration is used:
     * </p>
     * <ul>
     * <li>messageSerializer: {@link ObjectSerializationMessageSerializer}
     * <li>repositoryWatcherPollingSchedule: 30 seconds
     * <li>monitor: None are registered.
     * </ul>
     */
    public static final class Builder {
        private final MessageRepository messageRepository;
        private final TransactionSynchronizer transactionSynchronizer;
        private MessageSerializer messageSerializer = new ObjectSerializationMessageSerializer();
        private final List<Monitor<Event>> monitors = new ArrayList<Monitor<Event>>();
        private long repositoryWatcherPollingPeriod = 30;
        private TimeUnit repositoryWatcherPollingTimeUnit = TimeUnit.SECONDS;

        private Builder(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
            this.messageRepository = messageRepository;
            this.transactionSynchronizer = transactionSynchronizer;
        }

        /**
         * The {@link MessageSerializer} to use when serializing messages for storage in the repository and
         * deserializing when reading them from the repository.
         * <p>
         * If not specified, an {@link ObjectSerializationMessageSerializer} instance is used.
         * </p>
         *
         * @param messageSerializer the implementation to use. Must not be null.
         * @return the builder, so methods can be chained
         */
        public Builder messageSerializer(MessageSerializer messageSerializer) {
            Is.notNull(messageSerializer, "messageSerializer must not be null");
            this.messageSerializer = messageSerializer;
            return this;
        }

        /**
         * Specify how often the repository should be polled when looking for abandoned messages and
         * checking for queue size.
         * <p>
         * If not specified, the repository will be polled every 30 seconds.
         * </p>
         *
         * @param period   how often to poll. Must be creater than zero.
         * @param timeUnit the time unit of the period. Must not be null.
         * @return the builder, so methods can be chained
         */
        public Builder repositoryWatcherPollingSchedule(long period, TimeUnit timeUnit) {
            Is.greaterThenZero(period, "period must be greater than zero");
            Is.notNull(timeUnit, "timeUnit must not be null");
            this.repositoryWatcherPollingPeriod = period;
            this.repositoryWatcherPollingTimeUnit = timeUnit;
            return this;
        }

        /**
         * Registers a monitor, to be notified about events generated by the message broker.
         * This method can be called multiple times to register multiple monitors.
         * <p>
         * The core library doesn't do any logging itself, it's is strongly suggested to register a
         * logging monitor, such as {@code org.openforis.rmb.slf4j.Slf4jLoggingMonitor}.
         * </p>
         * <p>
         * If not called, no monitors will be registered.
         * </p>
         *
         * @param monitor the monitor to register
         * @return the builder, so methods can be chained
         */
        public Builder monitor(Monitor<Event> monitor) {
            Is.notNull(monitor, "monitor must not be null");
            monitors.add(monitor);
            return this;
        }

        /**
         * Builds the {@link RepositoryMessageBroker}, based on how the builder's been configured.
         *
         * @return the message broker instance
         */
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
