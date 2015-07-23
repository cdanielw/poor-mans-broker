package org.openforis.rmb;

public interface KeepAliveMessageHandler<M> {
    void handle(M message, KeepAlive keepAlive);
}
