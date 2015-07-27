package org.openforis.rmb

import org.openforis.rmb.inmemory.InMemoryMessageRepository
import org.openforis.rmb.monitor.MessageConsumptionFailedEvent
import org.openforis.rmb.monitor.RetryingMessageConsumptionEvent
import org.openforis.rmb.spi.TransactionSynchronizer
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import static org.openforis.rmb.spi.ThrottlingStrategy.NO_THROTTLING
import static org.openforis.rmb.spi.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER

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

    def 'Publishing a message without starting the message broker fails'() {
        def messageBroker = RepositoryMessageBroker
                .builder(new InMemoryMessageRepository(), NULL_TRANSACTION_SYNCHRONIZER)
                .build()
        def queue = messageBroker.queueBuilder('a queue')
                .consumer(MessageConsumer.builder('a consumer', {} as MessageHandler).build())
                .build()

        when:
            queue.publish('a message')

        then:
            thrown IllegalStateException
    }

    def 'Publishing a message without a transaction fails'() {
        def transactionSynchronizer = Mock(TransactionSynchronizer)
        def messageBroker = RepositoryMessageBroker
                .builder(new InMemoryMessageRepository(), transactionSynchronizer)
                .build()
        messageBroker.start()
        def queue = messageBroker.queueBuilder('a queue')
                .consumer(MessageConsumer.builder('a consumer', {} as MessageHandler))
                .build()


        transactionSynchronizer.inTransaction >> false

        when:
            queue.publish('a message')
        then:
            thrown IllegalStateException
    }

    def 'Registring a duplicate queue id fails'() {
        messageBroker.queueBuilder('duplicate id')
                .consumer(MessageConsumer.builder('consumer 1', {} as MessageHandler))
                .build()

        when:
            messageBroker.queueBuilder('duplicate id')
                    .consumer(MessageConsumer.builder('consumer 2', {} as MessageHandler))
                    .build()
        then:
            thrown IllegalArgumentException
    }

    def 'Registring a duplicate consumer id fails'() {
        when:
            messageBroker.queueBuilder('queue id')
                    .consumer(MessageConsumer.builder('duplicate id', {} as MessageHandler))
                    .consumer(MessageConsumer.builder('duplicate id', {} as MessageHandler))
                    .build()
        then:
            thrown IllegalArgumentException
    }

    def 'Creating a queue without a consumer fails'() {
        when:
            messageBroker.queueBuilder('queue id')
                    .build()
        then:
            thrown IllegalArgumentException
    }

    def 'Starting an already started broker fails'() {
        when:
            messageBroker.start()
        then:
            thrown IllegalStateException
    }

    def 'Starting a stopped broker fails'() {
        messageBroker.stop()

        when:
            messageBroker.start()
        then:
            thrown IllegalStateException
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
