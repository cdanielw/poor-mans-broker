package org.openforis.rmb.messagebroker

import org.openforis.rmb.messagebroker.spi.ThrottlingStrategy
import org.openforis.rmb.messagebroker.util.Clock
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class DefaultThrottlerTest extends Specification {
    def clock = new StaticClock()
    def throttlingStrategy = Mock(ThrottlingStrategy)
    def throttler = new Throttler.DefaultThrottler(clock)
    def timeout = 1000
    def consumer = MessageConsumer.builder('consumer id', {} as MessageHandler)
            .retry(throttlingStrategy)
            .timeout(timeout, TimeUnit.MILLISECONDS)
            .build()
    def keepAlive = Mock(KeepAlive)

    def 'Delays the amount of time determined by the throttling strategy'() {
        when:
            throttle(10)

        then:
            1 * throttlingStrategy.determineDelayMillis(10) >> 20
            clock.time == 20
    }

    def 'If delay is exactly half the timeout, keep alive is not sent'() {
        int delay = 500

        when:
            throttle(1)

        then:
            1 * throttlingStrategy.determineDelayMillis(1) >> delay
            0 * keepAlive.send()

            clock.time == delay
    }

    def 'If delay is one longer than half the timeout, keep alive is sent'() {
        int delay = 501

        when:
            throttle(1)

        then:
            1 * throttlingStrategy.determineDelayMillis(1) >> delay
            1 * keepAlive.send()

            clock.time == delay
    }

    def 'If delay is same the timeout, one keep alive is sent'() {
        int delay = timeout

        when:
            throttle(1)

        then:
            1 * throttlingStrategy.determineDelayMillis(1) >> delay
            1 * keepAlive.send()

            clock.time == delay
    }

    def 'If delay is one larger than the timeout, two keep alives are sent'() {
        int delay = timeout + 1

        when:
            throttle(1)

        then:
            1 * throttlingStrategy.determineDelayMillis(1) >> delay
            2 * keepAlive.send()

            clock.time == delay
    }

    void throttle(int retries) {
        throttler.throttle(retries, consumer, keepAlive)
    }

    static class StaticClock implements Clock {
        long time = 0

        long millis() {
            return time
        }

        void sleep(long millis) throws InterruptedException {
            time += millis
        }
    }
}
