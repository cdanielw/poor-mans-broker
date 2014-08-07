package com.wiell.messagebroker;

public interface RespondingMessageHandler<M, R> {
    R handle(M message);
}
