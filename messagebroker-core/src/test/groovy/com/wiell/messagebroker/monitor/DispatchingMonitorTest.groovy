package com.wiell.messagebroker.monitor

import com.wiell.messagebroker.MessageBrokerConfig
import com.wiell.messagebroker.PollingMessageBroker
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import spock.lang.Specification
import util.CollectingMonitor

import static com.wiell.messagebroker.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER

class DispatchingMonitorTest extends Specification {
    def 'Test'() {
        def startedMonitor = new CollectingMonitor<MessageBrokerStartedEvent>()
        def publishedMonitor = new CollectingMonitor<MessagePublishedEvent>()
        def dispatchingMonitor = DispatchingMonitor.builder()
                .monitor(MessageBrokerStartedEvent, startedMonitor)
                .monitor(MessagePublishedEvent, publishedMonitor)
                .build()

        def broker = new PollingMessageBroker(
                MessageBrokerConfig.builder(new InMemoryMessageRepository(), NULL_TRANSACTION_SYNCHRONIZER)
                        .monitor(dispatchingMonitor)
        )

        when:
            broker.start()
        then:
            startedMonitor.eventTypes() == [MessageBrokerStartedEvent]
            publishedMonitor.events.empty
    }
}
