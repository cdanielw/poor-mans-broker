package com.wiell.messagebroker;

public interface ThrottleStrategy {
    public static final ThrottleStrategy NO_THROTTLING = new ThrottleStrategy() {
        public int determineDelayMillis(int retry) {
            return 0;
        }
    };

    public static final ThrottleStrategy ONE_SECOND_PER_RETRY = new ThrottleStrategy() {
        public int determineDelayMillis(int retry) {
            return retry * 1000;
        }
    };

    int determineDelayMillis(int retry);
}
