package org.openforis.rmb.messagebroker;

public interface MessageBroker {
    RepositoryMessageBroker start();

    void stop();

    <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType);

    <M> MessageQueue.Builder<M> queueBuilder(String queueId);
}
