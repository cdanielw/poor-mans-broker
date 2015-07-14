package com.wiell.messagebroker.util;

public interface Clock {
    long millis();

    class SystemClock implements Clock {
        public long millis() {
            return System.currentTimeMillis();
        }
    }
}
