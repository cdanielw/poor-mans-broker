package util

import org.openforis.rmb.spi.Clock

import java.util.concurrent.TimeUnit

class AdjustableClock implements Clock {
    private final systemClock = new Clock.SystemClock()
    long offset = 0

    long millis() {
        return systemClock.millis() + offset
    }

    void sleep(long millis) throws InterruptedException {
        offset + millis
    }

    void inThePast(int time, TimeUnit timeUnit, Closure callback) {
        rewind(time, timeUnit)
        try {
            callback()
        } finally {
            reset()
        }
    }

    void rewind(int time, TimeUnit timeUnit) {
        offset = -timeUnit.toMillis(time)
    }

    void reset() {
        offset = 0
    }
}
