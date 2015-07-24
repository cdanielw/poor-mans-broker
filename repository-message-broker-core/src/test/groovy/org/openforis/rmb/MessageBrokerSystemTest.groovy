package org.openforis.rmb

import org.openforis.rmb.monitor.MessageConsumptionFailedEvent
import org.openforis.rmb.monitor.RetryingMessageConsumptionEvent
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import static org.openforis.rmb.spi.ThrottlingStrategy.NO_THROTTLING

class MessageBrokerSystemTest extends Specification {
    @SuppressWarnings("GroovyUnusedDeclaration")
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()

    def setup() {
        this.messagesHandledInParallel = 1
        queueTestDelegate.setUp()
    }

    def 'Message handler get a published message'() {
        def handler = createHandler()
        def queue = queue(handler)

        when:
            queue.publish('a message')

        then:
            handler.handled('a message')
    }

    def 'Given a never retrying consumer and a failing handler, consumption fails without retries'() {
        def handler = createFailingHandler(1)
        def queue = messageBroker.queueBuilder('queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler).neverRetry()
                .messagesHandledInParallel(1))
                .build()
        when:
            queue.publish('a message')
        then:
            consumptionFailed()
            retriesMade() == 0
    }

    def 'Given a consumer retrying once and a failing handler, consumption fails after one retry'() {
        def handler = createFailingHandler(2)
        def queue = messageBroker.queueBuilder('queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler).retry(1, NO_THROTTLING)
                .messagesHandledInParallel(1))
                .build()
        when:
            queue.publish('a message')
        then:
            consumptionFailed()
            retriesMade() == 1
    }

    private void consumptionFailed() {
        new PollingConditions().eventually {
            monitor.events.find { it.class == MessageConsumptionFailedEvent }
        }
    }

    private Number retriesMade() {
        monitor.events.count { it.class == RetryingMessageConsumptionEvent }
    }

}
