package org.openforis.rmb.messagebroker.spring;

import org.openforis.rmb.messagebroker.MessageBrokerConfig;
import org.openforis.rmb.messagebroker.MessageRepository;
import org.openforis.rmb.messagebroker.RepositoryMessageBroker;
import org.openforis.rmb.messagebroker.TransactionSynchronizer;


public final class SpringMessageBroker {
    final RepositoryMessageBroker messageBroker;

    public SpringMessageBroker(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
        messageBroker = new RepositoryMessageBroker(MessageBrokerConfig.builder(messageRepository, transactionSynchronizer));
    }
}
