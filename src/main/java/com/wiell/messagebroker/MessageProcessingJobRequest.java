package com.wiell.messagebroker;

public final class MessageProcessingJobRequest {
    public final String consumerId;
    public final int messageCount;

    public MessageProcessingJobRequest(String consumerId, int messageCount) {
        this.consumerId = consumerId;
        this.messageCount = messageCount;
    }
}
