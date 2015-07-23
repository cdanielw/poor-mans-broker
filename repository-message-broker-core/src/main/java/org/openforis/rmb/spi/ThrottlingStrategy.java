package org.openforis.rmb.spi;

import java.util.concurrent.TimeUnit;

public interface ThrottlingStrategy {
    ThrottlingStrategy NO_THROTTLING = new ThrottlingStrategy() {
        public int determineDelayMillis(int retry) {
            return 0;
        }
    };

    int determineDelayMillis(int retry);

    class ExponentialBackoff implements ThrottlingStrategy {
        private final int maxBackoffMillis;

        public ExponentialBackoff(int maxBackoff, TimeUnit timeUnit) {
            long max = timeUnit.toMillis(maxBackoff);
            if (max > Integer.MAX_VALUE)
                throw new IllegalArgumentException("Backoff cannot be longer than ...");
            this.maxBackoffMillis = (int) max;
        }

        public int determineDelayMillis(int retry) {
            if (retry > 24) return maxBackoffMillis;
            long backoff = (long) Math.pow(2, retry) * 100;
            return backoff > maxBackoffMillis ? maxBackoffMillis : (int) backoff;
        }
    }
}
