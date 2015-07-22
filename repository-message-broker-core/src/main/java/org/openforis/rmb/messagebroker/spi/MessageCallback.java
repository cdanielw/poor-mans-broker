package org.openforis.rmb.messagebroker.spi;

public interface MessageCallback {
    void messageTaken(MessageProcessingUpdate update, Object serializedMessage);
}
