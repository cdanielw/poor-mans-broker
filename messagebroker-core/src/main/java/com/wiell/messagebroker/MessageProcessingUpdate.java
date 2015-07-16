package com.wiell.messagebroker;

import java.util.UUID;

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.*;

public final class MessageProcessingUpdate<T> {
    public final String queueId;
    public final MessageConsumer<T> consumer;
    public final String messageId;
    public final Status fromStatus;
    public final Status toStatus;
    public final int retries;
    public final String errorMessage;
    public final String fromVersionId;
    public final String toVersionId;

    private MessageProcessingUpdate(
            String queueId,
            MessageConsumer<T> consumer,
            String messageId,
            Status fromStatus,
            Status toStatus,
            int retries,
            String errorMessage,
            String fromVersionId,
            String toVersionId) {
        this.queueId = queueId;
        this.consumer = consumer;
        this.messageId = messageId;
        this.fromStatus = fromStatus;
        this.toStatus = toStatus;
        this.retries = retries;
        this.errorMessage = errorMessage;
        this.fromVersionId = fromVersionId;
        this.toVersionId = toVersionId;
    }

    public static <T> MessageProcessingUpdate<T> create(
            String queueId,
            MessageConsumer<T> consumer,
            String messageId,
            Status fromStatus,
            Status toStatus,
            int retries,
            String errorMessage,
            String fromVersionId) {
        return new MessageProcessingUpdate<T>(queueId, consumer, messageId, fromStatus, toStatus, retries, errorMessage,
                fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> processing() {
        return new MessageProcessingUpdate<T>(queueId, consumer, messageId, toStatus, PROCESSING, retries, errorMessage,
                fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> completed() {
        return new MessageProcessingUpdate<T>(queueId, consumer, messageId, toStatus, COMPLETED, retries, errorMessage,
                fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> retry(String errorMessage) {
        return new MessageProcessingUpdate<T>(queueId, consumer, messageId, toStatus, PROCESSING, retries + 1,
                errorMessage, fromVersionId, newVersionId());
    }

    public MessageProcessingUpdate<T> failed(String errorMessage) {
        return new MessageProcessingUpdate<T>(queueId, consumer, messageId, toStatus, FAILED, retries, errorMessage,
                fromVersionId, newVersionId());
    }

    public String toString() {
        return "MessageProcessingUpdate{" +
                "queueId=" + queueId +
                ", consumer='" + consumer + '\'' +
                ", messageId='" + messageId + '\'' +
                ", fromStatus=" + fromStatus +
                ", toStatus=" + toStatus +
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
