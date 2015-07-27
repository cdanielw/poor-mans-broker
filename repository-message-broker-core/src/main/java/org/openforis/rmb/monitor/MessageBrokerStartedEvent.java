package org.openforis.rmb.monitor;

import org.openforis.rmb.MessageBroker;
import org.openforis.rmb.util.Is;

public final class MessageBrokerStartedEvent implements Event {
    public final MessageBroker messageBroker;

    public MessageBrokerStartedEvent(MessageBroker messageBroker) {
        Is.notNull(messageBroker, "messageBroker must not be null");
        this.messageBroker = messageBroker;
    }

    public String toString() {
        return "MessageBrokerStartedEvent{" +
                "messageBroker=" + messageBroker +
                '}';
    }
}
