package org.openforis.rmb.monitor;

import org.openforis.rmb.util.Is;

public class PollingForMessageQueueSizeChangesFailedEvent implements Event {
    public final Exception exception;

    public PollingForMessageQueueSizeChangesFailedEvent(Exception exception) {
        Is.notNull(exception, "exception must not be null");
        this.exception = exception;
    }

    public String toString() {
        return "PollingForMessageQueueSizeChangesFailedEvent{" +
                "exception=" + exception +
                '}';
    }
}
