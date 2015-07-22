package org.openforis.rmb.messagebroker

import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static org.openforis.rmb.messagebroker.spi.ThrottlingStrategy.ExponentialBackoff

class ExponentialBackoffTest extends Specification {
    def oneMinute = TimeUnit.MINUTES.toMillis(1)

    def 'Test'() {
        when:
            def delay = new ExponentialBackoff(1, TimeUnit.MINUTES).determineDelayMillis(retry)

        then:
            delay <= oneMinute
            delay > 0

        where:
            retry << (1..100)

    }
}
