package com.wiell.messagebroker;

public final class MessageResponse<M, R> {
    public final M message;
    public final R response;

    public MessageResponse(M message, R response) {
        this.message = message;
        this.response = response;
    }
}
