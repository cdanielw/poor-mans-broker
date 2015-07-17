package integration

import spock.lang.Specification

class QueueConcurrencyTest extends Specification {
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()

    def messages = 1..1000

    def setup() {
        workerCount = 4
        timeoutSecs = 5
    }

    def cleanup() {
        queueTestDelegate.cleanup()
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
}
