package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageConsumer;

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
