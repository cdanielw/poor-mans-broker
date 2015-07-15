package com.wiell.messagebroker;

import com.wiell.messagebroker.util.Clock;

interface Throttler {
    void throttle(int retry, MessageConsumer<?> consumer, KeepAlive keepAlive) throws InterruptedException;

    class DefaultThrottler implements Throttler {
        private final Clock clock;

        public DefaultThrottler(Clock clock) {
            this.clock = clock;
        }

        public void throttle(int retry, MessageConsumer<?> consumer, KeepAlive keepAlive) throws InterruptedException {
            long startTime = clock.millis();
            long totalDelay = consumer.throttlingStrategy.determineDelayMillis(retry);
            long expectedEndTime = startTime + totalDelay;
            long timeout = consumer.timeUnit.toMillis(consumer.timeout);
            long halfTimeout = timeout / 2;

            while (clock.millis() < expectedEndTime) {
                long delay = expectedEndTime - clock.millis();
                if (delay > halfTimeout) {
                    clock.sleep(halfTimeout);
                    keepAlive.send();
                } else
                    clock.sleep(delay);
            }
        }
    }
}