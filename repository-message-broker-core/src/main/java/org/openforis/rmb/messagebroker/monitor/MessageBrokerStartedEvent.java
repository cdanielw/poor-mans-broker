package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.RepositoryMessageBroker;

public final class MessageBrokerStartedEvent implements Event {
    public final RepositoryMessageBroker messageBroker;

    public MessageBrokerStartedEvent(RepositoryMessageBroker messageBroker) {
        this.messageBroker = messageBroker;
    }

    public String toString() {
        return "MessageBrokerStartedEvent{" +
                "messageBroker=" + messageBroker +
                '}';
    }
}
