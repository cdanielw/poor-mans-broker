package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.MessageConsumer;

import java.util.Map;

public class PollingForMessagesEvent implements Event {
    final Map<MessageConsumer<?>, Integer> maxCountByConsumer;

    public PollingForMessagesEvent(Map<MessageConsumer<?>, Integer> maxCountByConsumer) {
        this.maxCountByConsumer = maxCountByConsumer;
    }

    public String toString() {
        return "PollingForMessagesEvent{" +
                "maxCountByConsumer=" + maxCountByConsumer +
                '}';
    }
}
