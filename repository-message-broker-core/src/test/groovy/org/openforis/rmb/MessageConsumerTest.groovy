package org.openforis.rmb

import org.openforis.rmb.spi.ThrottlingStrategy
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class MessageConsumerTest extends Specification {
    def 'Builder configures MessageConsumer properly'() {
        def strategy = {} as ThrottlingStrategy
        when:
            def consumer = MessageConsumer.builder('consumer id', {} as MessageHandler)
                    .retry(11, strategy)
                    .messagesHandledInParallel(22)
                    .timeout(33, TimeUnit.HOURS)
                    .build()

        then:
            consumer.with {
                id == 'consumer id'
                maxRetries == 11
                throttlingStrategy == strategy
                messagesHandledInParallel == 22
                timeout == 33
            }
    }
}
