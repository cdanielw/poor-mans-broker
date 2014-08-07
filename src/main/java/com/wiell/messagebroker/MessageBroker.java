package com.wiell.messagebroker;

public interface MessageBroker {
    void start();

    void stop();

    <M> MessageQueue.Builder<M> queueWith(String queueId, Class<M> messageType);

    <M, R> RequestResponseMessageQueue.Builder<M, R> queueWith(String queueId, RespondingMessageHandler<M, R> handler);

}
