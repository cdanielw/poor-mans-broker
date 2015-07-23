package org.openforis.rmb;

public interface MessageHandler<M> {
    void handle(M message);
}
