package org.openforis.rmb.spi;

/**
 * A clock for measuring current time and sleeping.
 */
public interface Clock {
    /**
     * Gets the current time in milliseconds.
     *
     * @return current time in milliseconds
     */
    long millis();

    /**
     * Causes the current thread to sleep for the provided amount of milliseconds.
     *
     * @param millis the number of milliseconds to sleep fore
     * @throws InterruptedException when current thread is interrupted
     */
    void sleep(long millis) throws InterruptedException;

    /**
     * A default {@link Clock} implementation.
     */
    final class SystemClock implements Clock {
        public long millis() {
            return System.currentTimeMillis();
        }

        public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
        }
    }
}
