package com.wiell.messagebroker.spring;

import com.wiell.messagebroker.MessageBrokerConfig;
import com.wiell.messagebroker.MessageRepository;
import com.wiell.messagebroker.PollingMessageBroker;
import com.wiell.messagebroker.TransactionSynchronizer;


public final class SpringMessageBroker {
    final PollingMessageBroker messageBroker;

    public SpringMessageBroker(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
        messageBroker = new PollingMessageBroker(MessageBrokerConfig.builder(messageRepository, transactionSynchronizer));
    }
}
