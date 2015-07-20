package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageBroker;

public final class MessageBrokerStoppedEvent implements Event {
    public final MessageBroker messageBroker;

    public MessageBrokerStoppedEvent(MessageBroker messageBroker) {
        this.messageBroker = messageBroker;
    }

    public String toString() {
        return "MessageBrokerStoppedEvent{" +
                "messageBroker=" + messageBroker +
                '}';
    }
}
