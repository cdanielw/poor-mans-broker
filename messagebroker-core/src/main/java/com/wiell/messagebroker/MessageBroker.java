package com.wiell.messagebroker;

public interface MessageBroker {
    PollingMessageBroker start();

    void stop();

    <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType);
}
