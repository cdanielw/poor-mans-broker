package org.openforis.rmb.spring;

import org.openforis.rmb.MessageBrokerConfig;
import org.openforis.rmb.RepositoryMessageBroker;
import org.openforis.rmb.spi.MessageRepository;
import org.openforis.rmb.spi.TransactionSynchronizer;


public final class SpringMessageBroker {
    final RepositoryMessageBroker messageBroker;

    public SpringMessageBroker(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
        messageBroker = new RepositoryMessageBroker(MessageBrokerConfig.builder(messageRepository, transactionSynchronizer));
    }
}
