package org.openforis.rmb.messagebroker.spi;

public interface MessageProcessingCallback {
    void messageProcessing(MessageDetails messageDetails, MessageProcessingStatus status, Object serializedMessage);
}
