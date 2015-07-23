package org.openforis.rmb;

public interface MessageBroker {
    void start();

    void stop();

    <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType);

    <M> MessageQueue.Builder<M> queueBuilder(String queueId);
}
