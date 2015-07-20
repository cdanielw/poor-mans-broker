package org.openforis.rmb.messagebroker;

public interface MessageCallback {
    void messageTaken(MessageProcessingUpdate update, Object serializedMessage);
}
