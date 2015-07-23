package org.openforis.rmb.messagebroker.metrics

import com.codahale.metrics.MetricRegistry
import org.openforis.rmb.messagebroker.MessageConsumer
import org.openforis.rmb.messagebroker.MessageHandler
import org.openforis.rmb.messagebroker.monitor.*
import org.openforis.rmb.messagebroker.spi.Clock
import org.openforis.rmb.messagebroker.spi.MessageDetails
import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate
import spock.lang.Specification

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.FAILED


class MetricsMonitorTest extends Specification {
    def metrics = new MetricRegistry()
    def clock = new StaticClock()
    def monitor = new MetricsMonitor(metrics)
    def publicationTime = new Date(100)
    def message = 'A message'
    def e = new RuntimeException()


    def setup() {
        monitor.clock = clock
    }

    def 'MessageQueueCreatedEvent increases queueCount'() {
        when:
            monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('consumer id')]))
        then:
            metrics.counter('queueCount').count == 1
    }

    def 'MessagePublishedEvent increases queueId.messageCount and marks queueId.messageMeter'() {
        when:
            monitor.onEvent(new MessagePublishedEvent('someQueueId', message))
        then:
            metrics.counter('someQueueId.messageCount').count == 1
            metrics.meter('someQueueId.messageMeter').count == 1
    }

    def 'ConsumingNewMessageEvent updates queueId.consumerId.timesFromPublicationToTaken with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))
        then:
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToTaken').count == 1
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToTaken').snapshot.max == clock.time - publicationTime.time
    }

    def 'ConsumingNewMessageEvent increases queueId.consumerId.takenCount and marks consumerId.takenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 1
            metrics.meter('someQueueId.someConsumerId.takenMeter').count == 1
    }

    def 'ConsumingTimedOutMessageEvent increases queueId.consumerId.takenCount and marks consumerId.takenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), message))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 1
            metrics.meter('someQueueId.someConsumerId.takenMeter').count == 1
    }

    def 'ConsumingTimedOutMessageEvent increases queueId.consumerId.timedOutTakenCount and marks consumerId.timedOutTakenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), message))

        then:
            metrics.counter('someQueueId.someConsumerId.timedOutTakenCount').count == 1
            metrics.meter('someQueueId.someConsumerId.timedOutTakenMeter').count == 1
    }

    def 'RetryingMessageConsumptionEvent increments queueId.consumerId.retryCount and marks consumerId.retryMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))

        when:
            monitor.onEvent(new RetryingMessageConsumptionEvent(update('someConsumerId'), message, e))

        then:
            metrics.counter('someQueueId.someConsumerId.retryCount').count == 1
            metrics.meter('someQueueId.someConsumerId.retryMeter').count == 1
    }

    def 'MessageConsumedEvent decreases queueId.consumerId.takenCount, increases consumerId.completedCount, and marks consumerId.completedMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), message))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 0
            metrics.counter('someQueueId.someConsumerId.completedCount').count == 1
            metrics.meter('someQueueId.someConsumerId.completedMeter').count == 1
    }

    def 'MessageConsumptionFailedEvent decreases queueId.consumerId.takenCount, increases consumerId.failedCount, and marks consumerId.failedMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), message, e))

        then:
            metrics.counter('someQueueId.someConsumerId.takenCount').count == 0
            metrics.counter('someQueueId.someConsumerId.failedCount').count == 1
            metrics.meter('someQueueId.someConsumerId.failedMeter').count == 1
    }

    def 'MessageConsumedEvent updates queueId.consumerId.timesFromPublicationToCompletion with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), message))
        then:
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToCompletion').count == 1
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToCompletion').snapshot.max == clock.time - publicationTime.time
    }

    def 'MessageConsumptionFailedEvent updates queueId.consumerId.timesFromPublicationToFailure with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), message, e))
        then:
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToFailure').count == 1
            metrics.histogram('someQueueId.someConsumerId.timesFromPublicationToFailure').snapshot.max == clock.time - publicationTime.time
    }

    def 'Given a new message, MessageConsumedEvent updates queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), message))
        then:
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').snapshot.max == end - start
            metrics.histogram('someQueueId.someConsumerId.handlingTimes[java.lang.String]').count == 1
            metrics.histogram('someQueueId.someConsumerId.handlingTimes[java.lang.String]').snapshot.max == end - start
    }

    def 'Given a taken message, MessageConsumedEvent updates queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), message))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), message))
        then:
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.handlingTimes').snapshot.max == end - start
            metrics.histogram('someQueueId.someConsumerId.handlingTimes[java.lang.String]').count == 1
            metrics.histogram('someQueueId.someConsumerId.handlingTimes[java.lang.String]').snapshot.max == end - start
    }

    def 'ThrottlingMessageRetryEvent updates queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))
        def end = clock.time = 500

        when:
            monitor.onEvent(new ThrottlingMessageRetryEvent(update('someConsumerId'), message, e))
        then:
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').snapshot.max == end - start
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes[java.lang.String]').count == 1
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes[java.lang.String]').snapshot.max == end - start
    }

    def 'ScheduledMessagePollingFailedEvent updates queueId.consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), message))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), message, e))
        then:
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').count == 1
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes').snapshot.max == end - start
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes[java.lang.String]').count == 1
            metrics.histogram('someQueueId.someConsumerId.failingHandlingTimes[java.lang.String]').snapshot.max == end - start
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
        MessageProcessingUpdate.create(new MessageDetails('someQueueId', 'messageId', publicationTime), consumer(consumerId),
                new MessageProcessingStatus(FAILED, 0, null, new Date(0), 'fromVersionId'),
                new MessageProcessingStatus(FAILED, 0, null, new Date(0), UUID.randomUUID().toString()))
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
