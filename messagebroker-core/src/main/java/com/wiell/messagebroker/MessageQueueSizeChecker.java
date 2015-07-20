package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.MessageQueueSizeChangedEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MessageQueueSizeChecker {
    private final MessageRepository repository;
    private final Monitors monitors;
    private final Map<String, Integer> sizeByConsumerId = new ConcurrentHashMap<String, Integer>();
    private final Map<String, MessageConsumer<?>> consumerByConsumerId = new ConcurrentHashMap<String, MessageConsumer<?>>();
    private final Map<String, String> queueIdByConsumer = new ConcurrentHashMap<String, String>();

    MessageQueueSizeChecker(MessageRepository repository, Monitors monitors) {
        this.repository = repository;
        this.monitors = monitors;
    }


    void includeQueue(String queueId, List<MessageConsumer<?>> consumers) {
        for (MessageConsumer<?> consumer : consumers) {
            updateSize(consumer.id, -1);
            consumerByConsumerId.put(consumer.id, consumer);
            queueIdByConsumer.put(consumer.id, queueId);
        }
    }

    void check() {
        Map<String, Integer> sizeByConsumerId = repository.messageQueueSizeByConsumerId();
        for (String consumerId : this.sizeByConsumerId.keySet()) {
            Integer newSize = newSize(consumerId, sizeByConsumerId);
            if (hasSizeChanged(consumerId, newSize)) {
                updateSize(consumerId, newSize);
                notifyAboutSizeChange(consumerId);
            }
        }
    }

    private boolean hasSizeChanged(String consumerId, Integer newSize) {
        return !previousSize(consumerId).equals(newSize);
    }

    private Integer previousSize(String consumerId) {return this.sizeByConsumerId.get(consumerId);}

    private int newSize(String consumerId, Map<String, Integer> sizeByConsumerId) {
        Integer newSize = sizeByConsumerId.get(consumerId);
        return newSize == null ? 0 : newSize;
    }

    private void notifyAboutSizeChange(String consumerId) {
        String queueId = queueIdByConsumer.get(consumerId);
        MessageConsumer<?> consumer = consumerByConsumerId.get(consumerId);
        Integer size = previousSize(consumerId);
        monitors.onEvent(new MessageQueueSizeChangedEvent(queueId, consumer, size));
    }

    private Integer updateSize(String consumerId, Integer size) {
        return this.sizeByConsumerId.put(consumerId, size);
    }
}
