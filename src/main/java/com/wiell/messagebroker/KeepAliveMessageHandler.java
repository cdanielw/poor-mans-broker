package com.wiell.messagebroker;

public interface KeepAliveMessageHandler<M> {
    void handle(M message, KeepAlive keepAlive);
}
