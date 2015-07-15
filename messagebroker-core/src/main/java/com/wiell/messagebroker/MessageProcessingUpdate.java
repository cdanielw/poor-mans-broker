package com.wiell.messagebroker;

import java.util.UUID;

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.*;

public final class MessageProcessingUpdate<T> {
    public final MessageConsumer<T> consumer;
    public final String messageId;
    public final Status status;
    public final int retries;
    public final String errorMessage;
    public final String fromVersionId;
    public final String toVersionId;

    private MessageProcessingUpdate(
            MessageConsumer<T> consumer,
            String messageId, Status status,
            int retries,
            String errorMessage,
            String fromVersionId,
            String toVersionId) {
        this.consumer = consumer;
        this.messageId = messageId;
        this.status = status;
        this.retries = retries;
        this.errorMessage = errorMessage;
        this.fromVersionId = fromVersionId;
        this.toVersionId = toVersionId;
    }

    public static <T> MessageProcessingUpdate<T> create(
            MessageConsumer<T> consumer,
            String messageId,
            Status status,
            int retries,
            String errorMessage,
            String fromVersionId) {
        return new MessageProcessingUpdate<T>(consumer, messageId, status, retries, errorMessage, fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> processing() {
        return new MessageProcessingUpdate<T>(consumer, messageId, PROCESSING, retries, errorMessage, fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> completed() {
        return new MessageProcessingUpdate<T>(consumer, messageId, COMPLETED, retries, errorMessage, fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> retry(String errorMessage) {
        return new MessageProcessingUpdate<T>(consumer, messageId, PROCESSING, retries + 1, errorMessage, fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> failed(String errorMessage) {
        return new MessageProcessingUpdate<T>(consumer, messageId, FAILED, retries, errorMessage, fromVersionId, newVersionId());
    }

    public String toString() {
        return "MessageProcessingUpdate{" +
                "consumer=" + consumer +
                ", messageId='" + messageId + '\'' +
                ", status=" + status +
                ", retries=" + retries +
                ", errorMessage='" + errorMessage + '\'' +
                ", fromVersionId='" + fromVersionId + '\'' +
                ", toVersionId='" + toVersionId + '\'' +
                '}';
    }

    private static String newVersionId() {return UUID.randomUUID().toString();}

    public enum Status {
        PENDING, PROCESSING, COMPLETED, FAILED
    }
}
