package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.util.Is;

import java.util.Date;
import java.util.UUID;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*;

public final class MessageProcessingUpdate<T> {
    public final String queueId;
    public final String messageId;
    public final Date publicationTime;
    public final MessageConsumer<T> consumer;
    public final MessageProcessingStatus.State fromState;
    public final MessageProcessingStatus.State toState;
    public final int retries;
    public final String errorMessage;
    public final Date updateTime;
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
        this.updateTime = toStatus.lastUpdated;
        this.fromVersionId = fromStatus.versionId;
        this.toVersionId = toStatus.versionId;
    }

    public static <T> MessageProcessingUpdate<T> create(MessageDetails messageDetails,
                                                        MessageConsumer<T> consumer,
                                                        MessageProcessingStatus fromStatus,
                                                        MessageProcessingStatus toStatus) {
        return new MessageProcessingUpdate<T>(messageDetails, consumer, fromStatus, toStatus);
    }

    public MessageProcessingUpdate<T> processing(Clock clock) {
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(PROCESSING, retries, errorMessage, now(clock), randomUuid())
        );
    }

    public MessageProcessingUpdate<T> completed(Clock clock) {
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(COMPLETED, retries, errorMessage, now(clock), randomUuid())
        );
    }

    public MessageProcessingUpdate<T> retry(Clock clock, String errorMessage) {
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(PROCESSING, retries + 1, errorMessage, now(clock), randomUuid())
        );
    }

    public MessageProcessingUpdate<T> failed(Clock clock, String errorMessage) {
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(FAILED, retries, errorMessage, now(clock), randomUuid())
        );
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
                ", date=" + updateTime +
                ", fromVersionId='" + fromVersionId + '\'' +
                ", toVersionId='" + toVersionId + '\'' +
                '}';
    }

    private Date now(Clock clock) {
        return new Date(clock.millis());
    }

    private String randomUuid() {
        return UUID.randomUUID().toString();
    }

    private MessageDetails messageDetails() {
        return new MessageDetails(queueId, messageId, publicationTime);
    }

    private MessageProcessingStatus toStatus() {
        return new MessageProcessingStatus(toState, retries, errorMessage, updateTime, toVersionId);
    }

}
