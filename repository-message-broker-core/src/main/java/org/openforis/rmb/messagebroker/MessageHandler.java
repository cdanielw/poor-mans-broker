package org.openforis.rmb.messagebroker;

public interface MessageHandler<M> {
    void handle(M message);
}
