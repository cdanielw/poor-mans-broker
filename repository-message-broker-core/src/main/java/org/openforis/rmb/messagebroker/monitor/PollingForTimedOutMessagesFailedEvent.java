package org.openforis.rmb.messagebroker.monitor;

public class PollingForTimedOutMessagesFailedEvent implements Event {
    public final Exception exception;

    public PollingForTimedOutMessagesFailedEvent(Exception exception) {
        this.exception = exception;
    }

    public String toString() {
        return "PollingForTimedOutMessagesFailedEvent{" +
                "exception=" + exception +
                '}';
    }
}
