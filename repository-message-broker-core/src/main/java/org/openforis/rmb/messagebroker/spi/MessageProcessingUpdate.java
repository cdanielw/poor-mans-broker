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

    private MessageProcessingUpdate(MessageConsumer<T> consumer,
                                    MessageDetails messageDetails,
                                    MessageProcessingStatus fromStatus,
                                    MessageProcessingStatus toStatus) {
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
        validateState();
    }

    private void validateState() {
        Is.notNull(queueId, "queueId must not be null");
        Is.notNull(consumer, "consumer must not be null");
        Is.notNull(messageId, "messageId must not be null");
        Is.notNull(fromState, "fromStatus must not be null");
        Is.notNull(toState, "toStatus must not be null");
        Is.notNull(fromVersionId, "fromVersionId must not be null");
        Is.notNull(toVersionId, "toVersionId must not be null");
    }

    public static <T> MessageProcessingUpdate<T> createNew(MessageConsumer<T> consumer,
                                                           MessageDetails messageDetails) {
        return new MessageProcessingUpdate<T>(consumer, messageDetails,
                new MessageProcessingStatus(PENDING, 0, null),
                new MessageProcessingStatus(PENDING, 0, null));
    }

    public static <T> MessageProcessingUpdate<T> create(MessageConsumer<T> consumer,
                                                        MessageDetails messageDetails,
                                                        MessageProcessingStatus fromStatus,
                                                        MessageProcessingStatus toStatus) {
        return new MessageProcessingUpdate<T>(consumer, messageDetails, fromStatus, toStatus);
    }

    public static <T> MessageProcessingUpdate<T> take(MessageConsumer<T> consumer,
                                                      MessageDetails messageDetails,
                                                      MessageProcessingStatus fromStatus) {
        return new MessageProcessingUpdate<T>(consumer, messageDetails, fromStatus,
                new MessageProcessingStatus(PROCESSING, fromStatus.retries, fromStatus.errorMessage));
    }

    public MessageProcessingUpdate<T> processing() {
        return create(consumer, messageDetails(), toStatus(), new MessageProcessingStatus(PROCESSING, retries, errorMessage));
    }

    public MessageProcessingUpdate<T> completed() {
        return create(consumer, messageDetails(), toStatus(), new MessageProcessingStatus(COMPLETED, retries, errorMessage));
    }

    public MessageProcessingUpdate<T> retry(String errorMessage) {
        return create(consumer, messageDetails(), toStatus(), new MessageProcessingStatus(PROCESSING, retries + 1, errorMessage));
    }

    public MessageProcessingUpdate<T> failed(String errorMessage) {
        return create(consumer, messageDetails(), toStatus(), new MessageProcessingStatus(FAILED, retries, errorMessage));
    }

    public String toString() {
        return "MessageProcessingUpdate{" +
                "queueId=" + queueId +
                ", consumer='" + consumer + '\'' +
                ", messageId='" + messageId + '\'' +
                ", fromStatus=" + fromState +
                ", toStatus=" + toState +
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
