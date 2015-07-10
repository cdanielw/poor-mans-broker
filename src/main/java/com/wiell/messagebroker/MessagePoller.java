package com.wiell.messagebroker;

import com.wiell.messagebroker.MessageRepository.MessageCallback;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

final class MessagePoller {
    private final MessageRepository repository;
    private final MessageSerializer messageSerializer;
    private final ExecutorService messageTaker;
    private final ExecutorService workerExecutor;
    private final AtomicBoolean stopped = new AtomicBoolean();

    private ConcurrentHashMap<MessageConsumer<?>, AtomicInteger> currentlyProcessingMessageCountByConsumer =
            new ConcurrentHashMap<MessageConsumer<?>, AtomicInteger>();

    public MessagePoller(MessageRepository repository, MessageSerializer messageSerializer) {
        this.repository = repository;
        this.messageSerializer = messageSerializer;
        messageTaker = Executors.newSingleThreadExecutor(NamedThreadFactory.singleThreadFactory("messagebroker.MessageTaker"));
        workerExecutor = Executors.newCachedThreadPool(NamedThreadFactory.multipleThreadFactory("messagebroker.WorkerExecutor"));
    }

    public void registerConsumers(Collection<MessageConsumer<?>> consumers) {
        for (MessageConsumer<?> consumer : consumers) {
            currentlyProcessingMessageCountByConsumer.put(consumer, new AtomicInteger());
        }
    }

    public void poll() {
        if (stopped.get())
            return;
        messageTaker.execute(new Runnable() {
            public void run() {
                takeMessages();
            }
        });
    }

    private void takeMessages() {
        final Map<MessageConsumer<?>, Integer> maxCountByConsumer = determineMaxCountByConsumer();
        if (!maxCountByConsumer.isEmpty())
            repository.takeMessages(maxCountByConsumer, new MessageCallback() {
                public <T> void messageTaken(final MessageConsumer<T> consumer, final String messageId, final String serializedMessage) {
                    consume(consumer, messageId, serializedMessage);
                }
            });
    }

    private <T> void consume(final MessageConsumer<T> consumer, final String messageId, final String serializedMessage) {
        incrementCurrentlyProcessingMessageCount(consumer);

        workerExecutor.execute(new Runnable() {
            @SuppressWarnings("unchecked")
            public void run() {
                try {
                    T message = (T) messageSerializer.deserialize(serializedMessage);
                    new Worker(repository).consume(consumer, messageId, message);
                } finally {
                    decrementCurrentlyProcessingMessageCount(consumer);
                    poll();
                }
            }
        });
    }

    private void decrementCurrentlyProcessingMessageCount(MessageConsumer<?> consumer) {
        AtomicInteger messageCount = currentlyProcessingMessageCount(consumer);
        messageCount.decrementAndGet();
    }

    private void incrementCurrentlyProcessingMessageCount(MessageConsumer<?> consumer) {
        AtomicInteger messageCount = currentlyProcessingMessageCount(consumer);
        messageCount.incrementAndGet();
    }

    private AtomicInteger currentlyProcessingMessageCount(MessageConsumer<?> consumer) {
        return currentlyProcessingMessageCountByConsumer.get(consumer);
    }

    private Map<MessageConsumer<?>, Integer> determineMaxCountByConsumer() {
        Map<MessageConsumer<?>, Integer> maxCountByConsumer = new HashMap<MessageConsumer<?>, Integer>();
        for (Map.Entry<MessageConsumer<?>, AtomicInteger> entry : currentlyProcessingMessageCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            int messageCount = entry.getValue().get();
            int maxCount = consumer.workerCount - messageCount;
            if (maxCount > 0)
                maxCountByConsumer.put(consumer, maxCount);
        }
        return maxCountByConsumer;
    }

    public void stop() {
        stopped.set(true);
        messageTaker.shutdownNow();
        workerExecutor.shutdownNow();
    }
}
