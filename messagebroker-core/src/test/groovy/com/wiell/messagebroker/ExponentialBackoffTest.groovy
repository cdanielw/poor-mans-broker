package com.wiell.messagebroker

import spock.lang.Specification

import java.util.concurrent.TimeUnit

import static com.wiell.messagebroker.ThrottlingStrategy.ExponentialBackoff

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
