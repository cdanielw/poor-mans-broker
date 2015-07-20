package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.MessagePublishedEvent;
import com.wiell.messagebroker.monitor.MessageQueueCreatedEvent;
import com.wiell.messagebroker.monitor.ScheduledMessagePollingFailedEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

final class MessageQueueManager {
    private final MessageRepository repository;
    private final TransactionSynchronizer transactionSynchronizer;
    private final MessagePoller messagePoller;
    private final MessageSerializer messageSerializer;
    private final Monitors monitors;
    private final ScheduledExecutorService abandonedJobsFinder = Executors.newSingleThreadScheduledExecutor(
            NamedThreadFactory.singleThreadFactory("messagebroker.AbandonedJobsFinder")
    );
    private final long abondonedJobsFinderPeriod;
    private final TimeUnit abondonedJobsFinderTimeOut;

    private final Map<String, List<MessageConsumer<?>>> consumersByQueueId = new ConcurrentHashMap<String, List<MessageConsumer<?>>>();
    private final Set<String> consumerIds = new HashSet<String>(); // For asserting global consumer id uniqueness

    public MessageQueueManager(MessageBrokerConfig config) {
        this.repository = config.messageRepository;
        this.transactionSynchronizer = config.transactionSynchronizer;
        this.messagePoller = new MessagePoller(repository, config.messageSerializer, config.monitors);
        this.messageSerializer = config.messageSerializer;
        this.monitors = config.monitors;
        this.abondonedJobsFinderPeriod = config.abondonedJobsFinderPeriod;
        this.abondonedJobsFinderTimeOut = config.abondonedJobsFinderTimeUnit;
    }

    <M> void publish(String queueId, M message) {
        assertInTransaction();
        List<MessageConsumer<?>> consumers = consumersByQueueId.get(queueId);
        repository.add(queueId, consumers, messageSerializer.serialize(message));
        monitors.onEvent(new MessagePublishedEvent(queueId, message));
        pollForMessagesOnCommit();
    }

    void registerQueue(String queueId, List<MessageConsumer<?>> consumers) {
        assertConsumerUniqueness(consumers);
        consumersByQueueId.put(queueId, consumers);
        messagePoller.registerConsumers(consumers);
        monitors.onEvent(new MessageQueueCreatedEvent(queueId, consumers));
    }

    void start() {
        abandonedJobsFinder.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                try {
                    messagePoller.poll();
                } catch (Exception e) {
                    monitors.onEvent(new ScheduledMessagePollingFailedEvent(e));
                }
            }
        }, 0, abondonedJobsFinderPeriod, abondonedJobsFinderTimeOut);
    }

    void stop() {
        ExecutorTerminator.shutdownAndAwaitTermination(abandonedJobsFinder);
        messagePoller.stop();
    }

    private void pollForMessagesOnCommit() {
        transactionSynchronizer.notifyOnCommit(new TransactionSynchronizer.CommitListener() {
            public void committed() {
                messagePoller.poll();
            }
        });
    }

    private void assertInTransaction() {
        if (!transactionSynchronizer.isInTransaction())
            throw new NotInTransaction();
    }

    private void assertConsumerUniqueness(List<MessageConsumer<?>> consumers) {
        for (MessageConsumer<?> consumer : consumers)
            if (!consumerIds.add(consumer.id))
                throw new IllegalArgumentException("Duplicate consumer id: " + consumer.id);
    }
}
