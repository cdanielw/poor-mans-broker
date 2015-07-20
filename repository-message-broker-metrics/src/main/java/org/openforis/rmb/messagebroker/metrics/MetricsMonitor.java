package org.openforis.rmb.messagebroker.metrics;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.MessageProcessingUpdate;
import org.openforis.rmb.messagebroker.monitor.*;
import org.openforis.rmb.messagebroker.util.Clock;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static com.codahale.metrics.MetricRegistry.name;

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
        else if (event instanceof MessageQueueSizeChangedEvent)
            messageQueueSizeChangedEvent((MessageQueueSizeChangedEvent) event);
    }


    private void messageQueueCreated(MessageQueueCreatedEvent event) {
        metrics.counter("queueCount").inc();
        for (MessageConsumer<?> consumer : event.consumers) {
            messageHandlingTimesByConsumerId.put(consumer.id, new ConcurrentHashMap<String, Long>());
            metrics.register(QueueSizeGauge.getName(event.queueId, consumer), new QueueSizeGauge());
        }
    }

    private void messagePublished(MessagePublishedEvent event) {
        metrics.counter(name(event.queueId, "messageCount")).inc();
        metrics.meter(name(event.queueId, "messageMeter")).mark();
    }

    private void consumingNewMessage(ConsumingNewMessageEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.publicationTime;
        metrics.histogram(name(event.update.queueId, event.update.consumer.id, "timesFromPublicationToTaken"))
                .update(timeFromPublicationTime);
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "takenCount")).inc();
        metrics.meter(name(event.update.queueId, event.update.consumer.id, "takenMeter")).mark();
        recordMessage(event.update);
    }

    private void consumingTimedOutMessage(ConsumingTimedOutMessageEvent event) {
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "takenCount")).inc();
        metrics.meter(name(event.update.queueId, event.update.consumer.id, "takenMeter")).mark();
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "timedOutTakenCount")).inc();
        metrics.meter(name(event.update.queueId, event.update.consumer.id, "timedOutTakenMeter")).mark();
        recordMessage(event.update);
    }

    private void throttlingMessageRetry(ThrottlingMessageRetryEvent event) {
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(name(event.update.queueId, event.update.consumer.id, "failingHandlingTimes")).update(handlingTime);
    }

    private void retryingMessageConsumption(RetryingMessageConsumptionEvent event) {
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "retryCount")).inc();
        metrics.meter(name(event.update.queueId, event.update.consumer.id, "retryMeter")).mark();
    }

    private void messageConsumed(MessageConsumedEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.publicationTime;
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(name(event.update.queueId, event.update.consumer.id, "timesFromPublicationToCompletion"))
                .update(timeFromPublicationTime);
        metrics.histogram(name(event.update.queueId, event.update.consumer.id, "handlingTimes")).update(handlingTime);
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "takenCount")).dec();
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "completedCount")).inc();
        metrics.meter(name(event.update.queueId, event.update.consumer.id, "completedMeter")).mark();
        removeMessage(event.update);
    }

    private void messageConsumptionFailed(MessageConsumptionFailedEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.publicationTime;
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(name(event.update.queueId, event.update.consumer.id, "timesFromPublicationToFailure"))
                .update(timeFromPublicationTime);
        metrics.histogram(name(event.update.queueId, event.update.consumer.id, "failingHandlingTimes"))
                .update(handlingTime);
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "takenCount")).dec();
        metrics.counter(name(event.update.queueId, event.update.consumer.id, "failedCount")).inc();
        metrics.meter(name(event.update.queueId, event.update.consumer.id, "failedMeter")).mark();
        removeMessage(event.update);
    }

    private void messageQueueSizeChangedEvent(MessageQueueSizeChangedEvent event) {
        String name = QueueSizeGauge.getName(event.queueId, event.consumer);
        QueueSizeGauge gauge = (QueueSizeGauge) metrics.getGauges().get(name);
        if (gauge == null)
            throw new IllegalStateException("No gauge registered with name " + name);
        gauge.setSize(event.size);
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

    private static class QueueSizeGauge implements Gauge<Integer> {
        private AtomicInteger size = new AtomicInteger(-1);

        public Integer getValue() {
            return size.get();
        }

        void setSize(int size) {
            this.size.set(size);
        }

        static String getName(String queueId, MessageConsumer consumer) {
            return name(queueId, consumer.id, "queueSize");
        }
    }
}
