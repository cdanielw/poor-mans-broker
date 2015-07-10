package com.wiell.messagebroker;

public interface ThrottlingStrategy {
    ThrottlingStrategy NO_THROTTLING = new ThrottlingStrategy() {
        public int determineDelayMillis(int retry) {
            return 0;
        }
    };

    ThrottlingStrategy DELAY_ONE_SECOND_PER_RETRY = new ThrottlingStrategy() {
        public int determineDelayMillis(int retry) {
            return retry * 1000;
        }
    };

    int determineDelayMillis(int retry);
}
