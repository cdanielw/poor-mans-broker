package org.openforis.rmb.messagebroker.spi;

public interface MessageTakenCallback {
    void messageTaken(MessageProcessingUpdate update, Object serializedMessage);
}
