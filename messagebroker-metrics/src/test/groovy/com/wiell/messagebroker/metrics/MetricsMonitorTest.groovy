package com.wiell.messagebroker.metrics

import com.codahale.metrics.MetricRegistry
import com.wiell.messagebroker.MessageConsumer
import com.wiell.messagebroker.MessageHandler
import com.wiell.messagebroker.MessageProcessingUpdate
import com.wiell.messagebroker.monitor.*
import com.wiell.messagebroker.util.Clock
import spock.lang.Specification

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.FAILED

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


    def 'MessagePublishedEvent increase consumerId.queueSize for each consumer'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('consumer1'), consumer('consumer2')]))

        when:
            monitor.onEvent(new MessagePublishedEvent('someQueueId', null))

        then:
            metrics.counter('consumer1.queueSize').count == 1
            metrics.counter('consumer2.queueSize').count == 1
    }

    def 'ConsumingNewMessageEvent decreases consumerId.queueSize'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new MessagePublishedEvent('someQueueId', null))

        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        then:
            metrics.counter('someConsumerId.queueSize').count == 0
    }

    def 'ConsumingNewMessageEvent updates consumerId.timesFromPublicationToTaken with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someConsumerId.timesFromPublicationToTaken').count == 1
            metrics.histogram('someConsumerId.timesFromPublicationToTaken').snapshot.max == clock.time - publicationTime
    }

    def 'ConsumingNewMessageEvent increases consumerId.takenCount and marks consumerId.takenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someConsumerId.takenCount').count == 1
            metrics.meter('someConsumerId.takenMeter').count == 1
    }

    def 'ConsumingTimedOutMessageEvent increases consumerId.takenCount and marks consumerId.takenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someConsumerId.takenCount').count == 1
            metrics.meter('someConsumerId.takenMeter').count == 1
    }

    def 'ConsumingTimedOutMessageEvent increases consumerId.timedOutTakenCount and marks consumerId.timedOutTakenMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        when:
            monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someConsumerId.timedOutTakenCount').count == 1
            metrics.meter('someConsumerId.timedOutTakenMeter').count == 1
    }

    def 'RetryingMessageConsumptionEvent increments consumerId.retryCount and marks consumerId.retryMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new RetryingMessageConsumptionEvent(update('someConsumerId'), null, null))

        then:
            metrics.counter('someConsumerId.retryCount').count == 1
            metrics.meter('someConsumerId.retryMeter').count == 1
    }

    def 'MessageConsumedEvent decreases consumerId.takenCount, increases consumerId.completedCount, and marks consumerId.completedMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))

        then:
            metrics.counter('someConsumerId.takenCount').count == 0
            metrics.counter('someConsumerId.completedCount').count == 1
            metrics.meter('someConsumerId.completedMeter').count == 1
    }

    def 'MessageConsumptionFailedEvent decreases consumerId.takenCount, increases consumerId.failedCount, and marks consumerId.failedMeter'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), null, null))

        then:
            metrics.counter('someConsumerId.takenCount').count == 0
            metrics.counter('someConsumerId.failedCount').count == 1
            metrics.meter('someConsumerId.failedMeter').count == 1
    }

    def 'MessageConsumedEvent updates consumerId.timesFromPublicationToCompletion with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someConsumerId.timesFromPublicationToCompletion').count == 1
            metrics.histogram('someConsumerId.timesFromPublicationToCompletion').snapshot.max == clock.time - publicationTime
    }

    def 'MessageConsumptionFailedEvent updates consumerId.timesFromPublicationToFailure with time since publication time'() {
        clock.time = 300
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), null, null))
        then:
            metrics.histogram('someConsumerId.timesFromPublicationToFailure').count == 1
            metrics.histogram('someConsumerId.timesFromPublicationToFailure').snapshot.max == clock.time - publicationTime
    }

    def 'Given a new message, MessageConsumedEvent mark consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someConsumerId.handlingTimes').count == 1
            metrics.histogram('someConsumerId.handlingTimes').snapshot.max == end - start
    }

    def 'Given a taken message, MessageConsumedEvent mark consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingTimedOutMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumedEvent(update('someConsumerId'), null))
        then:
            metrics.histogram('someConsumerId.handlingTimes').count == 1
            metrics.histogram('someConsumerId.handlingTimes').snapshot.max == end - start
    }

    def 'ThrottlingMessageRetryEvent mark consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new ThrottlingMessageRetryEvent(update('someConsumerId'), null, null))
        then:
            metrics.histogram('someConsumerId.failingHandlingTimes').count == 1
            metrics.histogram('someConsumerId.failingHandlingTimes').snapshot.max == end - start
    }

    def 'ScheduledMessagePollingFailedEvent mark consumerId.handlingTimes with time since taking the event'() {
        monitor.onEvent(new MessageQueueCreatedEvent('someQueueId', [consumer('someConsumerId')]))

        def start = clock.time = 300
        monitor.onEvent(new ConsumingNewMessageEvent(update('someConsumerId'), null))
        def end = clock.time = 500

        when:
            monitor.onEvent(new MessageConsumptionFailedEvent(update('someConsumerId'), null, null))
        then:
            metrics.histogram('someConsumerId.failingHandlingTimes').count == 1
            metrics.histogram('someConsumerId.failingHandlingTimes').snapshot.max == end - start
    }

    private MessageProcessingUpdate update(String consumerId) {
        MessageProcessingUpdate.create('someQueueId', consumer(consumerId), 'messageId', publicationTime, FAILED, FAILED, 0, null, 'fromVersionId')
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
