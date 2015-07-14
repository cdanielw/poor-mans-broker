package com.wiell.messagebroker;

public interface MessageCallback {

    void messageTaken(MessageProcessingUpdate update, String serializedMessage);
}
