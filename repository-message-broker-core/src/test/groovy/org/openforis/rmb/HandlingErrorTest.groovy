package org.openforis.rmb

import spock.lang.Specification

class HandlingErrorTest extends Specification {
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()

    def setup() {
        queueTestDelegate.setUp()
    }

    def 'Given a retrying consumer, when handling fails, it will be retried'() {
        def handler = createFailingHandler(1)
        def queue = retryingQueue(1, handler)

        when:
            queue.publish('a message')

        then:
            handler.handled('a message')
    }

}
