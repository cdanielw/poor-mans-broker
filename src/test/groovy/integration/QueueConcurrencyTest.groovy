package integration

import com.wiell.messagebroker.*
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import spock.lang.Specification
import util.TestHandler

import static com.wiell.messagebroker.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER
import static groovyx.gpars.GParsPool.withPool

class QueueConcurrencyTest extends Specification {
    MessageBroker messageBroker = new PollingMessageBroker(
            MessageBrokerConfig.builder(
                    new InMemoryMessageRepository(),
                    NULL_TRANSACTION_SYNCHRONIZER
            )
    ).start()

    def messages = 1..20000
    def workerCount = 5
    def handlerDelayMillis = 0

    def cleanup() {
        messageBroker.stop()
    }

    def 'Message handler get a published message'() {
        def handler = createHandler()
        def queue = queue(handler)

        when:
            queue.publish('a message')

        then:
            handler.handled('a message')
    }

    def 'All messages are handled for a blocking queue'() {
        def handler = createHandler()
        def queue = blockingQueue(handler)

        when:
            publish(queue, messages)

        then:
            handler.handled(messages)
    }

    def 'All, concurrently published, messages are handled for a blocking queue'() {
        def handler = createHandler()
        def queue = blockingQueue(handler)

        when:
            publishInParallel(queue, messages)

        then:
            handler.handled(messages)
    }

    def 'All published messages are handled for a non-blocking queue'() {
        def handler = createHandler(workerCount)
        def queue = nonBlockingQueue(handler)

        when:
            publish(queue, messages)

        then:
            handler.handled(messages)
    }

    def 'All, concurrently published, messages are handled for a non-blocking queue'() {
        def handler = createHandler(workerCount)
        def queue = nonBlockingQueue(handler)

        when:
            publishInParallel(queue, messages)

        then:
            handler.handled(messages)
    }

    private TestHandler createHandler(int workerCount = 1) {
        new TestHandler(
                handlerDelayMillis: handlerDelayMillis,
                workerCount: workerCount,
                timeoutSecs: 5
        )
    }

    private List<Integer> publish(queue, IntRange messages) {
        messages.each {
            queue.publish(it)
        }
    }

    private Object publishInParallel(queue, messages) {
        withPool {
            messages.eachParallel {
                queue.publish(it)
            }
        }
    }

    private MessageQueue<Object> queue(TestHandler handler) {
        messageBroker.queueBuilder('queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler))
                .build()
    }

    private MessageQueue<Object> blockingQueue(TestHandler handler) {
        def consumer = MessageConsumer.builder('blocking consumer', handler).blocking()
        messageBroker.queueBuilder('blocking queue', Object)
                .consumer(consumer)
                .build()
    }

    private MessageQueue<Object> nonBlockingQueue(TestHandler handler) {
        def consumer = MessageConsumer.builder('non-blocking consumer', handler).nonBlocking(workerCount)
        messageBroker.queueBuilder('blocking queue', Object)
                .consumer(consumer)
                .build()
    }
}
