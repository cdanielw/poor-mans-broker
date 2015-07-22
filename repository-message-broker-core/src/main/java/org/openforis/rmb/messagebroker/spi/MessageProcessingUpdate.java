package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.util.Is;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*;

public final class MessageProcessingUpdate<T> {
    public final String queueId;
    public final String messageId;
    public final long publicationTime;
    public final MessageConsumer<T> consumer;
    public final MessageProcessingStatus.State fromState;
    public final MessageProcessingStatus.State toState;
    public final int retries;
    public final String errorMessage;
    public final String fromVersionId;
    public final String toVersionId;

    private MessageProcessingUpdate(MessageDetails messageDetails,
                                    MessageConsumer<T> consumer,
                                    MessageProcessingStatus fromStatus,
                                    MessageProcessingStatus toStatus) {
        Is.notNull(messageDetails, "messageDetails must not be null");
        Is.notNull(consumer, "consumer must not be null");
        Is.notNull(fromStatus, "fromStatus must not be null");
        Is.notNull(toStatus, "toStatus must not be null");
        this.queueId = messageDetails.queueId;
        this.consumer = consumer;
        this.messageId = messageDetails.messageId;
        this.publicationTime = messageDetails.publicationTime;
        this.fromState = fromStatus.state;
        this.toState = toStatus.state;
        this.retries = toStatus.retries;
        this.errorMessage = toStatus.errorMessage;
        this.fromVersionId = fromStatus.versionId;
        this.toVersionId = toStatus.versionId;
    }

    public static <T> MessageProcessingUpdate<T> create(MessageDetails messageDetails,
                                                        MessageConsumer<T> consumer,
                                                        MessageProcessingStatus fromStatus,
                                                        MessageProcessingStatus toStatus) {
        return new MessageProcessingUpdate<T>(messageDetails, consumer, fromStatus, toStatus);
    }

    public MessageProcessingUpdate<T> processing() {
        return create(messageDetails(), consumer, toStatus(), new MessageProcessingStatus(PROCESSING, retries, errorMessage));
    }

    public MessageProcessingUpdate<T> completed() {
        return create(messageDetails(), consumer, toStatus(), new MessageProcessingStatus(COMPLETED, retries, errorMessage));
    }

    public MessageProcessingUpdate<T> retry(String errorMessage) {
        return create(messageDetails(), consumer, toStatus(), new MessageProcessingStatus(PROCESSING, retries + 1, errorMessage));
    }

    public MessageProcessingUpdate<T> failed(String errorMessage) {
        return create(messageDetails(), consumer, toStatus(), new MessageProcessingStatus(FAILED, retries, errorMessage));
    }

    public String toString() {
        return "MessageProcessingUpdate{" +
                "queueId='" + queueId + '\'' +
                ", messageId='" + messageId + '\'' +
                ", publicationTime=" + publicationTime +
                ", consumer=" + consumer +
                ", fromState=" + fromState +
                ", toState=" + toState +
                ", retries=" + retries +
                ", errorMessage='" + errorMessage + '\'' +
                ", fromVersionId='" + fromVersionId + '\'' +
                ", toVersionId='" + toVersionId + '\'' +
                '}';
    }

    private MessageDetails messageDetails() {
        return new MessageDetails(queueId, messageId, publicationTime);
    }

    private MessageProcessingStatus toStatus() {
        return new MessageProcessingStatus(toState, retries, errorMessage, toVersionId);
    }

}
