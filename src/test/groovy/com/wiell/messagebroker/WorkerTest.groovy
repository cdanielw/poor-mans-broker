package com.wiell.messagebroker

import com.wiell.messagebroker.spi.MessageRepository
import integration.QueueTestDelegate
import spock.lang.Specification
import util.TestHandler

class WorkerTest extends Specification {
    @SuppressWarnings("GroovyUnusedDeclaration")
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()
    def repo = Mock(MessageRepository)
    def worker = new Worker(repo)
    private String messageId = 'message-id'
    private String message = 'message'

    def 'Passes message to consumer'() {
        def handler = createHandler()

        when:
            worker.consume(consumer(handler), messageId, message)

        then:
            handler.handled(message)

    }

    def 'Given a retrying consumer, when handling fails, it is retried'() {
        def handler = createFailingHandler(1)

        when:
            worker.consume(retryingConsumer(1, handler), messageId, message)

        then:
            handler.handled(message)
    }

    def 'Given a non-retrying consumer, when handling fails, repository is notified about the failure'() {
        def handler = createFailingHandler(1)
        def consumer = consumer(handler)

        when:
            worker.consume(consumer, messageId, message)

        then:
            1 * repo.failed(consumer, messageId, 0, _ as Exception)
            0 * repo._
    }

    def 'Given a retrying consumer, when handling fails, repository is notified about the retry'() {
        def handler = createFailingHandler(1)
        def consumer = retryingConsumer(1, handler)

        when:
            worker.consume(consumer, messageId, message)

        then:
            1 * repo.retrying(consumer, messageId, 1, _ as RuntimeException)
            0 * repo.failed(*_)
    }

    def 'Given a keep alive handler, when handler calls keep alive, repository is notified'() {
        def consumer = consumer(
                createHandler { message, KeepAlive keepAlive ->
                    keepAlive.send()
                }
        )

        when:
            worker.consume(consumer, messageId, message)

        then:
            1 * repo.keepAlive(consumer, messageId)
    }

    def 'When consumer successfully handled a message, repository is notified'() {
        def consumer = consumer(createHandler())

        when:
            worker.consume(consumer, messageId, message)

        then:
            1 * repo.completed(consumer, messageId)
            0 * repo._
    }


    private MessageConsumer consumer(TestHandler handler) {
        MessageConsumer.builder('consumer', handler).build()
    }

    private MessageConsumer retryingConsumer(int maxRetries, TestHandler handler) {
        MessageConsumer.builder('consumer', handler)
                .retry(maxRetries, ThrottlingStrategy.NO_THROTTLING)
                .build()
    }
}
