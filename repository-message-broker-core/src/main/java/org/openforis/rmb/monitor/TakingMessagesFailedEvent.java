package org.openforis.rmb.monitor;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.util.Is;

import java.util.Map;

public class TakingMessagesFailedEvent implements Event {
    public final Map<MessageConsumer<?>, Integer> maxCountByConsumer;
    public final Exception exception;

    public TakingMessagesFailedEvent(Map<MessageConsumer<?>, Integer> maxCountByConsumer, Exception exception) {
        Is.notEmpty(maxCountByConsumer, "maxCountByConsumer must not be empty");
        Is.notNull(exception, "exception must not be null");
        this.maxCountByConsumer = maxCountByConsumer;
        this.exception = exception;
    }

    public String toString() {
        return "TakingMessagesFailedEvent{" +
                "maxCountByConsumer=" + maxCountByConsumer +
                ", exception=" + exception +
                '}';
    }
}
