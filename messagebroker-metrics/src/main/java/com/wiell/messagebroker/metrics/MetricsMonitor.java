package com.wiell.messagebroker.metrics;


import com.codahale.metrics.MetricRegistry;
import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.MessageProcessingUpdate;
import com.wiell.messagebroker.monitor.*;
import com.wiell.messagebroker.util.Clock;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsMonitor implements Monitor<Event> {
    private final MetricRegistry metrics;
    private final Map<String, Map<String, Long>> messageHandlingTimesByConsumerId = new ConcurrentHashMap<String, Map<String, Long>>();
    private Clock clock = new Clock.SystemClock();

    public MetricsMonitor(MetricRegistry metrics) {
        this.metrics = metrics;
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    public void onEvent(Event event) {
        if (event instanceof MessageQueueCreatedEvent)
            messageQueueCreated((MessageQueueCreatedEvent) event);
        else if (event instanceof MessagePublishedEvent)
            messagePublished((MessagePublishedEvent) event);
        else if (event instanceof ConsumingNewMessageEvent)
            consumingNewMessage((ConsumingNewMessageEvent) event);
        else if (event instanceof ConsumingTimedOutMessageEvent)
            consumingTimedOutMessage((ConsumingTimedOutMessageEvent) event);
        else if (event instanceof ThrottlingMessageRetryEvent)
            throttlingMessageRetry((ThrottlingMessageRetryEvent) event);
        else if (event instanceof RetryingMessageConsumptionEvent)
            retryingMessageConsumption((RetryingMessageConsumptionEvent) event);
        else if (event instanceof MessageConsumedEvent)
            messageConsumed((MessageConsumedEvent) event);
        else if (event instanceof MessageConsumptionFailedEvent)
            messageConsumptionFailed((MessageConsumptionFailedEvent) event);

    }

    private void messageQueueCreated(MessageQueueCreatedEvent event) {
        metrics.counter("queueCount").inc();
        for (MessageConsumer<?> consumer : event.consumers)
            messageHandlingTimesByConsumerId.put(consumer.id, new ConcurrentHashMap<String, Long>());
    }

    private void messagePublished(MessagePublishedEvent event) {
        metrics.counter(event.queueId + ".messageCount").inc();
        metrics.meter(event.queueId + ".messageMeter").mark();
        for (String consumerId : consumerIds())
            metrics.counter(consumerId + ".queueSize").inc();
    }

    private void consumingNewMessage(ConsumingNewMessageEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.publicationTime;
        metrics.histogram(event.update.consumer.id + ".timesFromPublicationToTaken").update(timeFromPublicationTime);
        metrics.counter(event.update.consumer.id + ".takenCount").inc();
        metrics.meter(event.update.consumer.id + ".takenMeter").mark();
        metrics.counter(event.update.consumer.id + ".queueSize").dec();
        recordMessage(event.update);
    }

    private void consumingTimedOutMessage(ConsumingTimedOutMessageEvent event) {
        metrics.counter(event.update.consumer.id + ".takenCount").inc();
        metrics.meter(event.update.consumer.id + ".takenMeter").mark();
        metrics.counter(event.update.consumer.id + ".timedOutTakenCount").inc();
        metrics.meter(event.update.consumer.id + ".timedOutTakenMeter").mark();
        recordMessage(event.update);
    }

    private void throttlingMessageRetry(ThrottlingMessageRetryEvent event) {
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(event.update.consumer.id + ".failingHandlingTimes").update(handlingTime);
    }

    private void retryingMessageConsumption(RetryingMessageConsumptionEvent event) {
        metrics.counter(event.update.consumer.id + ".retryCount").inc();
        metrics.meter(event.update.consumer.id + ".retryMeter").mark();
    }

    private void messageConsumed(MessageConsumedEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.publicationTime;
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(event.update.consumer.id + ".timesFromPublicationToCompletion").update(timeFromPublicationTime);
        metrics.histogram(event.update.consumer.id + ".handlingTimes").update(handlingTime);
        metrics.counter(event.update.consumer.id + ".takenCount").dec();
        metrics.counter(event.update.consumer.id + ".completedCount").inc();
        metrics.meter(event.update.consumer.id + ".completedMeter").mark();
        removeMessage(event.update);
    }

    private void messageConsumptionFailed(MessageConsumptionFailedEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.publicationTime;
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(event.update.consumer.id + ".timesFromPublicationToFailure").update(timeFromPublicationTime);
        metrics.histogram(event.update.consumer.id + ".failingHandlingTimes").update(handlingTime);
        metrics.counter(event.update.consumer.id + ".takenCount").dec();
        metrics.counter(event.update.consumer.id + ".failedCount").inc();
        metrics.meter(event.update.consumer.id + ".failedMeter").mark();
        removeMessage(event.update);
    }

    private Set<String> consumerIds() {
        return messageHandlingTimesByConsumerId.keySet();
    }

    private void recordMessage(MessageProcessingUpdate update) {
        Map<String, Long> messages = messageHandlingTimesByConsumerId.get(update.consumer.id);
        messages.put(update.messageId, clock.millis());
    }

    private void removeMessage(MessageProcessingUpdate update) {
        Map<String, Long> messages = messageHandlingTimesByConsumerId.get(update.consumer.id);
        messages.remove(update.messageId);
    }

    private long handlingStartTime(MessageProcessingUpdate update) {
        Map<String, Long> messages = messageHandlingTimesByConsumerId.get(update.consumer.id);
        return messages.get(update.messageId);
    }
}
