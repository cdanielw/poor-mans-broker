package org.openforis.rmb;

import org.openforis.rmb.monitor.MessageQueueSizeChangedEvent;
import org.openforis.rmb.spi.MessageProcessingFilter;
import org.openforis.rmb.spi.MessageRepository;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.PENDING;
import static org.openforis.rmb.spi.MessageProcessingStatus.State.TIMED_OUT;

class MessageQueueSizeChecker {
    private final MessageRepository repository;
    private final Monitors monitors;
    private final Map<MessageConsumer<?>, Integer> sizeByConsumer = new ConcurrentHashMap<MessageConsumer<?>, Integer>();
    private final Map<MessageConsumer<?>, String> queueIdByConsumer = new ConcurrentHashMap<MessageConsumer<?>, String>();
    private final MessageProcessingFilter filter = MessageProcessingFilter.builder().states(PENDING, TIMED_OUT).build();

    private List<MessageConsumer<?>> consumers = new CopyOnWriteArrayList<MessageConsumer<?>>();

    MessageQueueSizeChecker(MessageRepository repository, Monitors monitors) {
        this.repository = repository;
        this.monitors = monitors;
    }

    void includeQueue(String queueId, List<MessageConsumer<?>> consumers) {
        this.consumers.addAll(consumers);
        for (MessageConsumer<?> consumer : consumers) {
            updateSize(consumer, -1);
            queueIdByConsumer.put(consumer, queueId);
        }
    }

    void check() {
        Map<MessageConsumer<?>, Integer> sizeByConsumer = repository.messageCountByConsumer(consumers, filter);
        for (MessageConsumer<?> consumer : this.sizeByConsumer.keySet()) {
            Integer newSize = newSize(consumer, sizeByConsumer);
            if (hasSizeChanged(consumer, newSize)) {
                updateSize(consumer, newSize);
                notifyAboutSizeChange(consumer);
            }
        }
    }

    private boolean hasSizeChanged(MessageConsumer<?> consumer, Integer newSize) {
        return !previousSize(consumer).equals(newSize);
    }

    private Integer previousSize(MessageConsumer<?> consumer) {return this.sizeByConsumer.get(consumer);}

    private int newSize(MessageConsumer<?> consumer, Map<MessageConsumer<?>, Integer> sizeByConsumer) {
        Integer newSize = sizeByConsumer.get(consumer);
        return newSize == null ? 0 : newSize;
    }

    private void notifyAboutSizeChange(MessageConsumer<?> consumer) {
        String queueId = queueIdByConsumer.get(consumer);
        Integer size = previousSize(consumer);
        monitors.onEvent(new MessageQueueSizeChangedEvent(queueId, consumer, size));
    }

    private Integer updateSize(MessageConsumer<?> consumer, Integer size) {
        return this.sizeByConsumer.put(consumer, size);
    }
}
