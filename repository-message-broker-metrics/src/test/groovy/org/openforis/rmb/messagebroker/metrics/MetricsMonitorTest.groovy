package org.openforis.rmb.messagebroker.metrics

import com.codahale.metrics.MetricRegistry
import org.openforis.rmb.messagebroker.MessageConsumer
import org.openforis.rmb.messagebroker.MessageHandler
import org.openforis.rmb.messagebroker.monitor.*
import org.openforis.rmb.messagebroker.spi.MessageDetails
import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate
import org.openforis.rmb.messagebroker.spi.Clock
import spock.lang.Specification

import static org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate.Status.FAILED

class MetricsMonitorTest extends Specification {
    def metrics = new MetricRegistry()
    def clock = new StaticClock()
    def monitor = new MetricsMonitor(metrics)
    def publicationTime = 100


    def setup() {
        monitor.clock = clock
    }

    def 'MessageQueueCreatedEvent increases queueCount'() {
        when:
            monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', []))
        then:
            metrics.counter('queueCount').count == 1
    }

    def 'MessagePublishedEvent increases queueId.messageCount and marks queueId.messageMeter'() {
        when:
            monitor.onEvent(new MessagePublishedEvent('someQueueId', null))
        then:
            metrics.counter('someQueueId.messageCount').count == 1
            metrics.meter('someQueueId.messageMeter').count == 1
    }

    def 'ConsumingNewMessageEvent updates queueId.consumerId.timesFromPublicationToTaken with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToTaken').count == 1
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToTaken').snapshot.max == clock.time - publicationTime
    }

    def 'ConsumingNewMessageEvent increases queueId.consumerId.takenCount and marks consumerId.takenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 1
            metrics.meter('someQueueId.someConsumerId.takenMeter').count == 1
    }

    def 'ConsumingTimedOutMessageEvent increases queueId.consumerId.takenCount and marks consumerId.takenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 1
            metrics.meter('someQueueId.someConsumerId.takenMeter').count == 1
    }

    def 'ConsumingTimedOutMessageEvent increases queueId.consumerId.timedOutTakenCount and marks consumerId.timedOutTakenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someQueueId.someConsumerId.timedOutTakenCount').count == 1
            metrics.meter('someQueueId.someConsumerId.timedOutTakenMeter').count == 1
    }

    def 'RetryingMessageConsumptionEvent increments queueId.consumerId.retryCount and marks consumerId.retryMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new RetryingMessageConsumptionEvent(update('someConsumerId'), null, null))

        then:
            metrics.counter('someQueueId.someConsumerId.retryCount').count == 1
            metrics.meter('someQueueId.someConsumerId.retryMeter').count == 1
    }

    def 'MessageConsumedEvent decreases queueId.consumerId.takenCount, increases consumerId.completedCount, and marks consumerId.completedMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 0
            metrics.counter('someQueueId.someConsumerId.completedCount').count == 1
            metrics.meter('someQueueId.someConsumerId.completedMeter').count == 1
    }

    def 'MessageConsumptionFailedEvent decreases queueId.consumerId.takenCount, increases consumerId.failedCount, and marks consumerId.failedMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), null, null))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 0
            metrics.counter('someQueueId.someConsumerId.failedCount').count == 1
            metrics.meter('someQueueId.someConsumerId.failedMeter').count == 1
    }

    def 'MessageConsumedEvent updates queueId.consumerId.timesFromPublicationToCompletion with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToCompletion').count == 1
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToCompletion').snapshot.max == clock.time - publicationTime
    }

    def 'MessageConsumptionFailedEvent updates queueId.consumerId.timesFromPublicationToFailure with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), null, null))
        then:
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToFailure').count == 1
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToFailure').snapshot.max == clock.time - publicationTime
    }

    def 'Given a new message, MessageConsumedEvent mark queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').snapshot.max == end - start
    }

    def 'Given a taken message, MessageConsumedEvent mark queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').snapshot.max == end - start
    }

    def 'ThrottlingMessageRetryEvent mark queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new ThrottlingMessageRetryEvent(update('someConsumerId'), null, null))
        then:
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').snapshot.max == end - start
    }

    def 'ScheduledMessagePollingFailedEvent mark queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), null, null))
        then:
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').snapshot.max == end - start
    }

    def 'MessageQueueSizeChangedEvent updates queueId.consumerId.queueSize gauge'() {
        def consumer = consumer('someConsumerId')
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer]))

        when:
            monitor.onEvent(new MessageQueueSizeChangedEvent('someQueueId', consumer, 1))

        then:
            metrics.gauges['someQueueId.someConsumerId.queueSize'].value == 1
    }

    private MessageProcessingUpdate update(String consumerId) {
        MessageProcessingUpdate.create(consumer(consumerId), new MessageDetails('someQueueId', 'messageId', publicationTime),
                new MessageProcessingStatus(FAILED, 0, null, 'fromVersionId'),
                new MessageProcessingStatus(FAILED, 0, null))
    }

    MessageConsumer consumer(String consumerId) {
        MessageConsumer.builder(consumerId, {} as MessageHandler).build()
    }

    static class StaticClock implements Clock {
        long time = 0

        long millis() {
            return time
        }

        void sleep(long millis) throws InterruptedException {
            time += millis
        }
    }
}
