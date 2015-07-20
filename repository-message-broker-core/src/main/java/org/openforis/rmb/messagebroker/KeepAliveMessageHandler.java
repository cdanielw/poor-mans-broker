package org.openforis.rmb.messagebroker;

public interface KeepAliveMessageHandler<M> {
    void handle(M message, KeepAlive keepAlive);
}
