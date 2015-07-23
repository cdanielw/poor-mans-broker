package org.openforis.rmb.monitor;

import org.openforis.rmb.MessageBroker;
import org.openforis.rmb.util.Is;

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
