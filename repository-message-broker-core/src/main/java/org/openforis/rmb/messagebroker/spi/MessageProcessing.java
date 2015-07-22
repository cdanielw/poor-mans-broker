package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.util.Is;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.PROCESSING;

public final class MessageProcessing<T> {
    public final String queueId;
    public final String messageId;
    public final long publicationTime;
    public final MessageConsumer<T> consumer;
    public final MessageProcessingStatus.State state;
    public final int retries;
    public final String errorMessage;
    public final String versionId;

    private MessageProcessing(MessageDetails messageDetails,
                              MessageConsumer<T> consumer,
                              MessageProcessingStatus status) {
        Is.notNull(messageDetails, "messageDetails must not be null");
        Is.notNull(consumer, "consumer must not be null");
        Is.notNull(status, "status must not be null");
        this.queueId = messageDetails.queueId;
        this.consumer = consumer;
        this.messageId = messageDetails.messageId;
        this.publicationTime = messageDetails.publicationTime;
        this.state = status.state;
        this.retries = status.retries;
        this.errorMessage = status.errorMessage;
        this.versionId = status.versionId;
    }

    public MessageProcessingUpdate<T> take() {
        return MessageProcessingUpdate.create(messageDetails(), consumer, status(),
                new MessageProcessingStatus(PROCESSING, retries, errorMessage));
    }

    private MessageProcessingStatus status() {
        return new MessageProcessingStatus(state, retries, errorMessage, versionId);
    }

    private MessageDetails messageDetails() {
        return new MessageDetails(queueId, messageId, publicationTime);
    }

    public String toString() {
        return "MessageProcessing{" +
                "queueId='" + queueId + '\'' +
                ", messageId='" + messageId + '\'' +
                ", publicationTime=" + publicationTime +
                ", consumer=" + consumer +
                ", state=" + state +
                ", retries=" + retries +
                ", errorMessage='" + errorMessage + '\'' +
                ", versionId='" + versionId + '\'' +
                '}';
    }

    public static <T> MessageProcessing<T> create(MessageDetails messageDetails,
                                                  MessageConsumer<T> consumer,
                                                  MessageProcessingStatus status) {
        return new MessageProcessing<T>(messageDetails, consumer, status);
    }
}
