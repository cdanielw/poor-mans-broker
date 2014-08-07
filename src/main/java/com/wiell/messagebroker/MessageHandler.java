package com.wiell.messagebroker;

public interface MessageHandler<M> {
    void handle(M message);
}
