package integration

import org.openforis.rmb.messagebroker.*
import org.openforis.rmb.messagebroker.inmemory.InMemoryMessageRepository
import util.CollectingMonitor
import util.TestHandler

import static TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER
import static groovyx.gpars.GParsPool.withPool

class QueueTestDelegate {
    CollectingMonitor monitor = new CollectingMonitor()
    MessageBroker messageBroker = new RepositoryMessageBroker(
            MessageBrokerConfig.builder(
                    new InMemoryMessageRepository(),
                    NULL_TRANSACTION_SYNCHRONIZER
            )
                    .monitor(monitor)
    ).start()

    int messagesHandledInParallel = 5
    int handlerDelayMillis = 0
    int randomHandlerDelayMillis = 0
    int timeoutSecs = 1

    def cleanup() {
        messageBroker.stop()
    }

    MessageQueue<Object> retryingQueue(int retries, TestHandler handler) {
        messageBroker.queueBuilder('queue', Object).consumer(
                MessageConsumer.builder('consumer', handler)
                        .messagesHandledInParallel(1)
                        .retry(retries, ThrottlingStrategy.NO_THROTTLING)
        ).build()
    }

    TestHandler createFailingHandler(int failOnUntilRetry, String errorMessage = 'Error message') {
        int retry = 0
        createHandler(1) {
            if (retry++ < failOnUntilRetry)
                throw new IllegalStateException(errorMessage)
        }
    }

    TestHandler createHandler(Closure handler) {
        createHandler(1, handler)
    }

    TestHandler createHandler(int messagesHandledInParallel = 1, Closure handler = null) {
        new TestHandler(
                handlerDelayMillis: handlerDelayMillis,
                randomHandlerDelayMillis: randomHandlerDelayMillis,
                messagesHandledInParallel: messagesHandledInParallel,
                timeoutSecs: timeoutSecs,
                handler: handler
        )
    }

    MessageQueue<Object> queue(TestHandler handler) {
        messageBroker.queueBuilder('queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler)
                .messagesHandledInParallel(1))
                .build()
    }

    MessageQueue<Object> blockingQueue(TestHandler handler) {
        def consumer = MessageConsumer.builder('blocking consumer', handler).messagesHandledInParallel(1)
        messageBroker.queueBuilder('blocking queue', Object)
                .consumer(consumer)
                .build()
    }

    MessageQueue<Object> nonBlockingQueue(TestHandler handler) {
        def consumer = MessageConsumer.builder('non-blocking consumer', handler).messagesHandledInParallel(messagesHandledInParallel)
        messageBroker.queueBuilder('blocking queue', Object)
                .consumer(consumer)
                .build()
    }


    List<Integer> publish(queue, IntRange messages) {
        messages.each {
            queue.publish(it)
        }
    }

    Object publishInParallel(queue, messages) {
        withPool {
            messages.eachParallel {
                queue.publish(it)
            }
        }
    }
}
