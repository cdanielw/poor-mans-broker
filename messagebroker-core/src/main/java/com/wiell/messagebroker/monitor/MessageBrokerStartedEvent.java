package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.PollingMessageBroker;

public final class MessageBrokerStartedEvent implements Event {
    public final PollingMessageBroker messageBroker;

    public MessageBrokerStartedEvent(PollingMessageBroker messageBroker) {
        this.messageBroker = messageBroker;
    }

    public String toString() {
        return "MessageBrokerStartedEvent{" +
                "messageBroker=" + messageBroker +
                '}';
    }
}
