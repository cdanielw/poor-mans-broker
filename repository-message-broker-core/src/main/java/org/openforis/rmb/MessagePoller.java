package org.openforis.rmb;

import org.openforis.rmb.monitor.MessageUpdateConflictEvent;
import org.openforis.rmb.monitor.PollingForMessagesEvent;
import org.openforis.rmb.monitor.TakingMessagesFailedEvent;
import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingUpdate;
import org.openforis.rmb.spi.MessageRepository;
import org.openforis.rmb.spi.MessageSerializer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.openforis.rmb.NamedThreadFactory.multipleThreadFactory;
import static org.openforis.rmb.NamedThreadFactory.singleThreadFactory;
import static org.openforis.rmb.Throttler.DefaultThrottler;

final class MessagePoller {
    private final MessageRepository repository;
    private final MessageSerializer messageSerializer;
    private final Monitors monitors;
    private final ExecutorService messageTaker;
    private final ExecutorService workerExecutor;
    private final Throttler throttler = new DefaultThrottler(new Clock.SystemClock());

    private ConcurrentHashMap<MessageConsumer<?>, AtomicInteger> currentlyProcessingMessageCountByConsumer =
            new ConcurrentHashMap<MessageConsumer<?>, AtomicInteger>();

    MessagePoller(MessageRepository repository, MessageSerializer messageSerializer, Monitors monitors) {
        this.repository = repository;
        this.messageSerializer = messageSerializer;
        this.monitors = monitors;
        messageTaker = Executors.newSingleThreadExecutor(singleThreadFactory("rmb.MessageTaker"));
        workerExecutor = Executors.newCachedThreadPool(multipleThreadFactory("rmb.WorkerExecutor"));
    }

    void registerConsumers(Collection<MessageConsumer<?>> consumers) {
        for (MessageConsumer<?> consumer : consumers) {
            currentlyProcessingMessageCountByConsumer.put(consumer, new AtomicInteger());
        }
    }

    void poll() {
        messageTaker.execute(new Runnable() {
            public void run() {
                takeMessages();
            }
        });
    }

    private void takeMessages() {
        final Map<MessageConsumer<?>, Integer> maxCountByConsumer = determineMaxCountByConsumer();
        if (maxCountByConsumer.isEmpty())
            return;
        try {
            monitors.onEvent(new PollingForMessagesEvent(maxCountByConsumer));
            repository.take(maxCountByConsumer, new MessageRepository.MessageTakenCallback() {
                public void taken(MessageProcessingUpdate update, Object serializedMessage) {
                    consume(update, serializedMessage);
                }
            });
        } catch (Exception e) {
            monitors.onEvent(new TakingMessagesFailedEvent(maxCountByConsumer, e));
        }
    }

    private <M> void consume(final MessageProcessingUpdate<M> update, final Object serializedMessage) {
        incrementCurrentlyProcessingMessageCount(update.getConsumer());

        workerExecutor.execute(new Runnable() {
            @SuppressWarnings("unchecked")
            public void run() {
                M message = (M) messageSerializer.deserialize(serializedMessage);
                try {
                    new Worker<M>(repository, throttler, monitors, update, message).consume();
                } catch (InterruptedException ignore) {
                    Thread.currentThread().interrupt();
                } catch (Worker.MessageUpdateConflict e) {
                    monitors.onEvent(new MessageUpdateConflictEvent(update, message));
                } finally {
                    decrementCurrentlyProcessingMessageCount(update.getConsumer());
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
            int maxCount = consumer.messagesHandledInParallel - messageCount;
            if (maxCount > 0)
                maxCountByConsumer.put(consumer, maxCount);
        }
        return maxCountByConsumer;
    }

    void stop() {
        ExecutorTerminator.shutdownAndAwaitTermination(messageTaker, workerExecutor);
    }
}
