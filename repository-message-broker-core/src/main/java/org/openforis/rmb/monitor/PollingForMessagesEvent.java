package org.openforis.rmb.monitor;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.util.Is;

import java.util.Map;

public final class PollingForMessagesEvent implements Event {
    public final Map<MessageConsumer<?>, Integer> maxCountByConsumer;

    public PollingForMessagesEvent(Map<MessageConsumer<?>, Integer> maxCountByConsumer) {
        Is.notNull(maxCountByConsumer, "maxCountByConsumer must not be null");
        this.maxCountByConsumer = maxCountByConsumer;
    }

    public String toString() {
        return "PollingForMessagesEvent{" +
                "maxCountByConsumer=" + maxCountByConsumer +
                '}';
    }
}
