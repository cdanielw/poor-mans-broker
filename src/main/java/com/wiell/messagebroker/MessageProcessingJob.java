package com.wiell.messagebroker;

public final class MessageProcessingJob {
    public final String messageId;
    public final Object message;
    public final String consumerId;
    //  TODO: Add status

    public MessageProcessingJob(String messageId, Object message, String consumerId) {
        this.messageId = messageId;
        this.message = message;
        this.consumerId = consumerId;
    }

    public interface Callback {
        void onJob(MessageProcessingJob job);
    }
}
