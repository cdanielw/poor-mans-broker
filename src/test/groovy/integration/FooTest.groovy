package integration

import com.wiell.messagebroker.*
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import groovyx.gpars.GParsPool
import integration.util.BlockingVar
import spock.lang.Ignore
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import static com.wiell.messagebroker.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER

class FooTest extends Specification {
    MessageBroker messageBroker = new PollingMessageBroker(
            MessageBrokerConfig.builder(
                    new InMemoryMessageRepository(),
                    NULL_TRANSACTION_SYNCHRONIZER
            )
    ).start()

    def cleanup() {
        messageBroker?.stop()
    }

    @Ignore
    def 'Message handler get a published message'() {
        def handler = new TestHandler()
        def queue = queue(handler)

        when:
            queue.publish('a message')

        then:
            handler.lastHandled('a message')
    }

    @Ignore
    def 'All messages are handled for a blocking queue'() {
        def handler = new TestHandler()
        def queue = blockingQueue(handler)

        def messageCount = 10000

        when:
            (1..messageCount).each {
                queue.publish(it)
            }

        then:
            handler.handledAll((1..messageCount))
            println handler.messagesHandled.size()
    }

    def 'All, concurrently published, messages are handled for a blocking queue'() {
        def handler = new TestHandler()
        def queue = blockingQueue(handler)

        def messageCount = 10000

        when:
            GParsPool.withPool {
                (1..messageCount).eachParallel {
                    queue.publish(it)
                }
            }

        then:
            handler.handledAll((1..messageCount))
            println handler.messagesHandled.size()
    }

    private MessageQueue<Object> queue(TestHandler handler) {
        messageBroker.queueBuilder('queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler))
                .build()
    }

    private MessageQueue<Object> blockingQueue(TestHandler handler) {
        messageBroker.queueBuilder('blocking queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler)
                .blocking())
                .build()
    }

    static class TestHandler<T> implements MessageHandler<T> {
        private final lastHandledMessage = new BlockingVar<T>('Handler got no messages')
        private final currentlyExecuting = new AtomicInteger()
        final ConcurrentHashMap<T, Boolean> messagesHandled = new ConcurrentHashMap()

        void handle(T message) {
            def value = currentlyExecuting.incrementAndGet()
            if (value != 1)
                throw new IllegalStateException("More then one executing at the same time")
            messagesHandled[message] = true
            lastHandledMessage.set(message)
            def afterValue = currentlyExecuting.decrementAndGet()
            if (afterValue != 0)
                throw new IllegalStateException("More then one executing at the same time")
        }

        void lastHandled(T message) {
            def lastHandledMessage = this.lastHandledMessage.get()
            assert message == lastHandledMessage
        }

        void handledAll(Collection<T> messages) {
            def conds = new PollingConditions()
            conds.timeout = 2
            conds.eventually {
                println messagesHandled.size()
                assert messages.size() >= messagesHandled.size()
                assert messagesHandled.keySet().containsAll(messages)
            }
        }
    }
}
