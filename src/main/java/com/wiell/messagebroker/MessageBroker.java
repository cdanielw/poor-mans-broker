package com.wiell.messagebroker;

public interface MessageBroker {
    void start();

    void stop();

    <M> MessageQueue.Builder<M> queue(Class<M> messageType);

    <M, R> RequestResponseMessageQueue.Builder<M, R> requestResponseQueue(RespondingMessageHandler<M, R> handler);
}
