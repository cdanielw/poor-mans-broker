package org.openforis.rmb.monitor;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.util.Is;

import java.util.Collection;

public class CheckingForMessageQueueSizeChangesFailedEvent implements Event {
    public final Collection<MessageConsumer<?>> consumers;
    public final Exception exception;

    public CheckingForMessageQueueSizeChangesFailedEvent(Collection<MessageConsumer<?>> consumers, Exception exception) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(exception, "exception must not be null");
        this.consumers = consumers;
        this.exception = exception;
    }

    public String toString() {
        return "PollingForMessageQueueSizeChangesFailedEvent{" +
                "exception=" + exception +
                '}';
    }
}
