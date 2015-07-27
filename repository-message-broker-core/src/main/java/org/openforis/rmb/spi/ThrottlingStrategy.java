package org.openforis.rmb.spi;

import java.util.concurrent.TimeUnit;

/**
 * A strategy on how message consumption retries should be throttled.
 */
public interface ThrottlingStrategy {
    /**
     * A strategy where no delay is applied between retries.
     */
    ThrottlingStrategy NO_THROTTLING = new ThrottlingStrategy() {
        public int determineDelayMillis(int retry) {
            return 0;
        }
    };

    /**
     * The delay in milliseconds to wait before retrying.
     *
     * @param retry the number of retries that previously been made + 1
     * @return the delay in milliseconds
     */
    int determineDelayMillis(int retry);

    /**
     * A strategy applying an exponential backoff, with a configurable maximum wait time.
     */
    final class ExponentialBackoff implements ThrottlingStrategy {
        private static final int MAXIMUM_RETRY_BEFORE_OVERFLOWING = 24;
        private final int maxBackoffMillis;

        /**
         * Creates an exponential backoff strategy.
         *
         * @param maxBackoff the maximum delay
         * @param timeUnit   the time unit of the maximum delay
         */
        public ExponentialBackoff(int maxBackoff, TimeUnit timeUnit) {
            long max = timeUnit.toMillis(maxBackoff);
            if (max > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Backoff cannot be longer than ...");
            this.maxBackoffMillis = (int) max;
        }

        public int determineDelayMillis(int retry) {
            if (retry > MAXIMUM_RETRY_BEFORE_OVERFLOWING) return maxBackoffMillis;
            long backoff = (long) Math.pow(2, retry) * 100;
            return backoff > maxBackoffMillis ? maxBackoffMillis : (int) backoff;
        }
    }
}
