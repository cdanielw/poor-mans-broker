package org.openforis.rmb.messagebroker.spi;

public interface Clock {
    long millis();

    void sleep(long millis) throws InterruptedException;

    final class SystemClock implements Clock {
        public long millis() {
            return System.currentTimeMillis();
        }

        public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }
}
