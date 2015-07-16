package com.wiell.messagebroker.monitor;

public class ScheduledMessagePollingFailedEvent implements Event {
    public final Exception exception;

    public ScheduledMessagePollingFailedEvent(Exception exception) {
        this.exception = exception;
    }
}
