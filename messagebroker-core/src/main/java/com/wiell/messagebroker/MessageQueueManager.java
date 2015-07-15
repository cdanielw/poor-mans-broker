package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.MessageBrokerStartedEvent;
import com.wiell.messagebroker.monitor.MessageBrokerStoppedEvent;
import com.wiell.messagebroker.monitor.MessagePublishedEvent;
import com.wiell.messagebroker.monitor.MessageQueueCreatedEvent;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

final class MessageQueueManager {
    private final MessageRepository repository;
    private final TransactionSynchronizer transactionSynchronizer;
    private final MessagePoller messagePoller;
    private final MessageSerializer messageSerializer;
    private final Monitors monitors;

    private final Map<String, List<MessageConsumer<?>>> consumersByQueueId = new ConcurrentHashMap<String, List<MessageConsumer<?>>>();
    private final Set<String> consumerIds = new HashSet<String>(); // For asserting global consumer id uniqueness

    public MessageQueueManager(MessageBrokerConfig config) {
        this.repository = config.messageRepository;
        this.transactionSynchronizer = config.transactionSynchronizer;
        this.messagePoller = new MessagePoller(repository, config.messageSerializer, config.monitors);
        this.messageSerializer = config.messageSerializer;
        this.monitors = config.monitors;
    }

    <M> void publish(String queueId, M message) {
        assertInTransaction();
        List<MessageConsumer<?>> consumers = consumersByQueueId.get(queueId);
        repository.add(queueId, consumers, messageSerializer.serialize(message));
        monitors.onEvent(new MessagePublishedEvent<M>(queueId, message));
        pollForMessagesOnCommit();
    }

    void registerQueue(String queueId, List<MessageConsumer<?>> consumers) {
        assertConsumerUniqueness(consumers);
        consumersByQueueId.put(queueId, consumers);
        messagePoller.registerConsumers(consumers);
        monitors.onEvent(new MessageQueueCreatedEvent(queueId, consumers));
    }

    void start() {
        monitors.onEvent(new MessageBrokerStartedEvent());
    }

    void stop() {
        messagePoller.stop();
        monitors.onEvent(new MessageBrokerStoppedEvent());
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
