package integration

import spock.lang.Specification

class HandlingErrorTest extends Specification {
    @SuppressWarnings("GroovyUnusedDeclaration")
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()

    def 'Given a retrying consumer, when handling fails, it will be retried'() {
        def handler = createFailingHandler(1)
        def queue = retryingQueue(1, handler)

        when:
            queue.publish('a message')

        then:
            handler.handled('a message')
    }

}
