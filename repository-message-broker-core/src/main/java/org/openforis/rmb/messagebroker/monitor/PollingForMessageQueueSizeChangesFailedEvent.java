package org.openforis.rmb.messagebroker.monitor;

public class PollingForMessageQueueSizeChangesFailedEvent implements Event {
    public final Exception exception;

    public PollingForMessageQueueSizeChangesFailedEvent(Exception exception) {
        this.exception = exception;
    }

    public String toString() {
        return "PollingForMessageQueueSizeChangesFailedEvent{" +
                "exception=" + exception +
                '}';
    }
}
