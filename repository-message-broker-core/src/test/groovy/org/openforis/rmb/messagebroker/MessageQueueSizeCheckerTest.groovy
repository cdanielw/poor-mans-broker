package org.openforis.rmb.messagebroker

import org.openforis.rmb.messagebroker.monitor.MessageQueueSizeChangedEvent
import org.openforis.rmb.messagebroker.monitor.Monitor
import org.openforis.rmb.messagebroker.spi.MessageRepository
import spock.lang.Specification

class MessageQueueSizeCheckerTest extends Specification {
    def monitor = Mock(Monitor)
    def monitors = new Monitors([monitor])
    def messageRepository = Mock(MessageRepository)
    def watcher = new MessageQueueSizeChecker(messageRepository, monitors)

    def 'Checking without any sizes does not notify monitor'() {
        messageRepository.messageQueueSizeByConsumerId() >> [:]

        when:
            watcher.check()

        then:
            0 * monitor.onEvent(_)
    }

    def 'Given a queue with no messages, when checking, monitor is notified'() {
        MessageQueueSizeChangedEvent event = null
        def consumer = consumer('consumer id')
        watcher.includeQueue('queue id', [consumer])
        messageRepository.messageQueueSizeByConsumerId() >> [:]

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
        messageRepository.messageQueueSizeByConsumerId() >> ['consumer id': 2]
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
        messageRepository.messageQueueSizeByConsumerId() >> ['consumer 1': 1, 'consumer 2': 2]
        def events = [] as List<MessageQueueSizeChangedEvent>

        when:
            watcher.check()

        then:
            2 * monitor.onEvent({ events << it } as MessageQueueSizeChangedEvent)
            events.size() == 2

            events[0].consumer == consumer1
            events[0].queueId == 'queue id'
            events[0].size == 1

            events[1].consumer == consumer2
            events[1].queueId == 'queue id'
            events[1].size == 2
    }

    def 'Given two queues with a consumer each with one and two messages, when checking, monitor is notified for each consumer'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        watcher.includeQueue('queue 1', [consumer1])
        watcher.includeQueue('queue 2', [consumer2])
        messageRepository.messageQueueSizeByConsumerId() >> ['consumer 1': 1, 'consumer 2': 2]
        def events = [] as List<MessageQueueSizeChangedEvent>

        when:
            watcher.check()

        then:
            2 * monitor.onEvent({ events << it } as MessageQueueSizeChangedEvent)
            events.size() == 2

            events[0].consumer == consumer1
            events[0].queueId == 'queue 1'
            events[0].size == 1

            events[1].consumer == consumer2
            events[1].queueId == 'queue 2'
            events[1].size == 2
    }

    private MessageConsumer consumer(String consumerId) {
        MessageConsumer.builder(consumerId, {} as MessageHandler).build()
    }
}
