package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageBroker;
import org.openforis.rmb.messagebroker.util.Is;

public final class MessageBrokerStoppedEvent implements Event {
    public final MessageBroker messageBroker;

    public MessageBrokerStoppedEvent(MessageBroker messageBroker) {
        Is.notNull(messageBroker, "messageBroker must not be null");
        this.messageBroker = messageBroker;
    }

    public String toString() {
        return "MessageBrokerStoppedEvent{" +
                "messageBroker=" + messageBroker +
                '}';
    }
}
