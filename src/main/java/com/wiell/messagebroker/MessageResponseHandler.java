package com.wiell.messagebroker;

public interface MessageResponseHandler<M, R> extends MessageHandler<MessageResponse<M, R>> {
    void handle(MessageResponse<M, R> messageResponse);
}
