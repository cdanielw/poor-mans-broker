package com.wiell.messagebroker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

final class MessageQueueManager {
    private final MessageRepository repository;
    private final TransactionSynchronizer transactionSynchronizer;
    private final MessagePoller messagePoller;
    private final MessageSerializer messageSerializer;

    private final Map<String, List<MessageConsumer<?>>> consumersByQueueId = new ConcurrentHashMap<String, List<MessageConsumer<?>>>();
    private final Set<String> consumerIds = new HashSet<String>(); // For asserting global consumer id uniqueness

    public MessageQueueManager(MessageBrokerConfig config) {
        this.repository = config.messageRepository;
        this.transactionSynchronizer = config.transactionSynchronizer;
        this.messagePoller = new MessagePoller(repository, config.messageSerializer);
        this.messageSerializer = config.messageSerializer;
    }

    <M> void publish(String queueId, M message) {
        assertInTransaction();
        List<MessageConsumer<?>> consumers = consumersByQueueId.get(queueId);
        repository.addMessage(queueId, consumers, messageSerializer.serialize(message));
        pollForMessagesOnCommit();
    }

    void registerQueue(String queueId, List<MessageConsumer<?>> consumers) {
        assertConsumerUniqueness(consumers);
        consumersByQueueId.put(queueId, consumers);
        messagePoller.registerConsumers(consumers);
    }

    void start() {
        // TODO: Schedule abandoned jobs check
    }

    void stop() {
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
            throw new NotInTransaction("Trying to publish message outside of a transaction");
    }

    private void assertConsumerUniqueness(List<MessageConsumer<?>> consumers) {
        for (MessageConsumer<?> consumer : consumers)
            if (!consumerIds.add(consumer.id))
                throw new IllegalArgumentException("Duplicate consumer id: " + consumer.id);
    }
}
