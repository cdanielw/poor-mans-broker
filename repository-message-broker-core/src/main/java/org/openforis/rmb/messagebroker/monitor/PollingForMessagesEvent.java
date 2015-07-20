package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageConsumer;

import java.util.Map;

public final class PollingForMessagesEvent implements Event {
    public final Map<MessageConsumer<?>, Integer> maxCountByConsumer;

    public PollingForMessagesEvent(Map<MessageConsumer<?>, Integer> maxCountByConsumer) {
        this.maxCountByConsumer = maxCountByConsumer;
    }

    public String toString() {
        return "PollingForMessagesEvent{" +
                "maxCountByConsumer=" + maxCountByConsumer +
                '}';
    }
}
