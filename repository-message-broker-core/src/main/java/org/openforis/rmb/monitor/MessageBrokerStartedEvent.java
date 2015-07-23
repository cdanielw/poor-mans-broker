package org.openforis.rmb.monitor;

import org.openforis.rmb.RepositoryMessageBroker;
import org.openforis.rmb.util.Is;

public final class MessageBrokerStartedEvent implements Event {
    public final RepositoryMessageBroker messageBroker;

    public MessageBrokerStartedEvent(RepositoryMessageBroker messageBroker) {
        Is.notNull(messageBroker, "messageBroker must not be null");
        this.messageBroker = messageBroker;
    }

    public String toString() {
        return "MessageBrokerStartedEvent{" +
                "messageBroker=" + messageBroker +
                '}';
    }
}
