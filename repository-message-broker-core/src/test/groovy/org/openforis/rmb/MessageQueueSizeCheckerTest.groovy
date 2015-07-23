package org.openforis.rmb

import org.openforis.rmb.monitor.MessageQueueSizeChangedEvent
import org.openforis.rmb.monitor.Monitor
import org.openforis.rmb.spi.MessageProcessingFilter
import org.openforis.rmb.spi.MessageRepository
import spock.lang.Specification

class MessageQueueSizeCheckerTest extends Specification {
    def monitor = Mock(Monitor)
    def monitors = new Monitors([monitor])
    def messageRepository = Mock(MessageRepository)
    def watcher = new MessageQueueSizeChecker(messageRepository, monitors)

    def 'Checking without any sizes does not notify monitor'() {
        repositoryReturns([:])

        when:
            watcher.check()

        then:
            0 * monitor.onEvent(_)
    }

    def 'Given a queue with no messages, when checking, monitor is notified'() {
        MessageQueueSizeChangedEvent event = null
        def consumer = consumer('consumer id')
        watcher.includeQueue('queue id', [consumer])
        repositoryReturns([:])

        when:
            watcher.check()

        then:
            1 * monitor.onEvent({ event = it } as MessageQueueSizeChangedEvent)
            event.consumer == consumer
            event.queueId == 'queue id'
            event.size == 0
    }

    def 'Given a queue with two messages, when checking, monitor is notified'() {
        def consumer = consumer('consumer id')
        watcher.includeQueue('queue id', [consumer])
        repositoryReturns((consumer): 2)
        MessageQueueSizeChangedEvent event = null

        when:
            watcher.check()

        then:
            1 * monitor.onEvent({ event = it } as MessageQueueSizeChangedEvent)
            event.consumer == consumer
            event.queueId == 'queue id'
            event.size == 2
    }

    def 'Given a queue with two consumers with one and two messages, when checking, monitor is notified for each consumer'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        watcher.includeQueue('queue id', [consumer1, consumer2])
        repositoryReturns((consumer1): 1, (consumer2): 2)
        def events = [] as List<MessageQueueSizeChangedEvent>

        when:
            watcher.check()

        then:
            2 * monitor.onEvent({ events << it } as MessageQueueSizeChangedEvent)
            events.size() == 2

            def event1 = events.find { it.consumer == consumer1 }
            event1.queueId == 'queue id'
            event1.size == 1

            def event2 = events.find { it.consumer == consumer2 }
            event2.queueId == 'queue id'
            event2.size == 2
    }

    def 'Given two queues with a consumer each with one and two messages, when checking, monitor is notified for each consumer'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        watcher.includeQueue('queue 1', [consumer1])
        watcher.includeQueue('queue 2', [consumer2])
        repositoryReturns((consumer1): 1, (consumer2): 2)
        def events = [] as List<MessageQueueSizeChangedEvent>

        when:
            watcher.check()

        then:
            2 * monitor.onEvent({ events << it } as MessageQueueSizeChangedEvent)
            events.size() == 2

            def event1 = events.find { it.consumer == consumer1 }
            event1.queueId == 'queue 1'
            event1.size == 1

            def event2 = events.find { it.consumer == consumer2 }
            event2.queueId == 'queue 2'
            event2.size == 2
    }

    void repositoryReturns(Map<MessageConsumer, Integer> counts) {
        messageRepository.messageCountByConsumer(_ as List, _ as MessageProcessingFilter) >> counts
    }

    private MessageConsumer consumer(String consumerId) {
        MessageConsumer.builder(consumerId, {} as MessageHandler).build()
    }
}
