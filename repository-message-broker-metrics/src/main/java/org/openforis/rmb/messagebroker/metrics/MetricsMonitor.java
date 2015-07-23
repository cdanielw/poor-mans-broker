package org.openforis.rmb.messagebroker.metrics;


import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.monitor.*;
import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;

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
            messageHandlingTimesByConsumerId.put(consumer.getId(), new ConcurrentHashMap<String, Long>());
            metrics.register(QueueSizeGauge.getName(event.queueId, consumer), new QueueSizeGauge());
        }
    }

    private void messagePublished(MessagePublishedEvent event) {
        metrics.counter(name(event.queueId, "messageCount")).inc();
        metrics.meter(name(event.queueId, "messageMeter")).mark();
    }

    private void consumingNewMessage(ConsumingNewMessageEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.getPublicationTime().getTime();
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "timesFromPublicationToTaken"))
                .update(timeFromPublicationTime);
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "takenCount")).inc();
        metrics.meter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "takenMeter")).mark();
        recordMessage(event.update);
    }

    private void consumingTimedOutMessage(ConsumingTimedOutMessageEvent event) {
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "takenCount")).inc();
        metrics.meter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "takenMeter")).mark();
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "timedOutTakenCount")).inc();
        metrics.meter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "timedOutTakenMeter")).mark();
        recordMessage(event.update);
    }

    private void throttlingMessageRetry(ThrottlingMessageRetryEvent event) {
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "failingHandlingTimes")).update(handlingTime);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "failingHandlingTimes["
                + event.message.getClass().getName() + "]")).update(handlingTime);
    }

    private void retryingMessageConsumption(RetryingMessageConsumptionEvent event) {
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "retryCount")).inc();
        metrics.meter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "retryMeter")).mark();
    }

    private void messageConsumed(MessageConsumedEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.getPublicationTime().getTime();
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "timesFromPublicationToCompletion"))
                .update(timeFromPublicationTime);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "handlingTimes")).update(handlingTime);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "handlingTimes["
                + event.message.getClass().getName() + "]")).update(handlingTime);
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "takenCount")).dec();
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "completedCount")).inc();
        metrics.meter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "completedMeter")).mark();
        removeMessage(event.update);
    }

    private void messageConsumptionFailed(MessageConsumptionFailedEvent event) {
        long timeFromPublicationTime = clock.millis() - event.update.getPublicationTime().getTime();
        long handlingTime = clock.millis() - handlingStartTime(event.update);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "timesFromPublicationToFailure"))
                .update(timeFromPublicationTime);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "failingHandlingTimes"))
                .update(handlingTime);
        metrics.histogram(name(event.update.getQueueId(), event.update.getConsumer().getId(), "failingHandlingTimes["
                + event.message.getClass().getName() + "]")).update(handlingTime);
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "takenCount")).dec();
        metrics.counter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "failedCount")).inc();
        metrics.meter(name(event.update.getQueueId(), event.update.getConsumer().getId(), "failedMeter")).mark();
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
        Map<String, Long> messages = messageHandlingTimesByConsumerId.get(update.getConsumer().getId());
        messages.put(update.getMessageId(), clock.millis());
    }

    private void removeMessage(MessageProcessingUpdate update) {
        Map<String, Long> messages = messageHandlingTimesByConsumerId.get(update.getConsumer().getId());
        messages.remove(update.getMessageId());
    }

    private long handlingStartTime(MessageProcessingUpdate update) {
        Map<String, Long> messages = messageHandlingTimesByConsumerId.get(update.getConsumer().getId());
        return messages.get(update.getMessageId());
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
            return name(queueId, consumer.getId(), "queueSize");
        }
    }
}
