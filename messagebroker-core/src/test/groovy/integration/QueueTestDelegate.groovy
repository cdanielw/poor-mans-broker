package integration

import com.wiell.messagebroker.*
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import util.TestHandler

import static TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER
import static groovyx.gpars.GParsPool.withPool

class QueueTestDelegate {
    MessageBroker messageBroker = new PollingMessageBroker(
            MessageBrokerConfig.builder(
                    new InMemoryMessageRepository(),
                    NULL_TRANSACTION_SYNCHRONIZER
            )
    ).start()

    int workerCount = 5
    int handlerDelayMillis = 0
    int randomHandlerDelayMillis = 0
    int timeoutSecs = 1

    def cleanup() {
        messageBroker.stop()
    }


    MessageQueue<Object> retryingQueue(int retries, TestHandler handler) {
        messageBroker.queueBuilder('queue', Object).consumer(
                MessageConsumer.builder('consumer', handler)
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

    TestHandler createHandler(int workerCount = 1, Closure handler = null) {
        new TestHandler(
                handlerDelayMillis: handlerDelayMillis,
                randomHandlerDelayMillis: randomHandlerDelayMillis,
                workerCount: workerCount,
                timeoutSecs: timeoutSecs,
                handler: handler
        )
    }

    MessageQueue<Object> queue(TestHandler handler) {
        messageBroker.queueBuilder('queue', Object)
                .consumer(MessageConsumer.builder('consumer', handler))
                .build()
    }

    MessageQueue<Object> blockingQueue(TestHandler handler) {
        def consumer = MessageConsumer.builder('blocking consumer', handler).blocking()
        messageBroker.queueBuilder('blocking queue', Object)
                .consumer(consumer)
                .build()
    }

    MessageQueue<Object> nonBlockingQueue(TestHandler handler) {
        def consumer = MessageConsumer.builder('non-blocking consumer', handler).nonBlocking(workerCount)
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
