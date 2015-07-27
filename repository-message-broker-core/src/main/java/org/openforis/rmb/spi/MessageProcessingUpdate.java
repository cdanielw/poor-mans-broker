package org.openforis.rmb.spi;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.MessageProcessingStatus.State;
import org.openforis.rmb.util.Is;

import java.util.Date;
import java.util.UUID;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.*;

/**
 * Represents the change of status in a message to be/being processed by a consumer.
 * <p>
 * Instances of this class is created through
 * {@link #create(MessageDetails, MessageConsumer, MessageProcessingStatus, MessageProcessingStatus)}.
 * </p>
 * <p>
 * This class is immutable.
 * </p>
 *
 * @param <T> the type of the message
 */
public final class MessageProcessingUpdate<T> {
    final String queueId;
    final String messageId;
    final Date publicationTime;
    final MessageConsumer<T> consumer;
    final MessageProcessingStatus.State fromState;
    final MessageProcessingStatus.State toState;
    final int retries;
    final String errorMessage;
    final Date updateTime;
    final String fromVersionId;
    final String toVersionId;

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

    /**
     * Creates an instance.
     *
     * @param messageDetails the details about the message
     * @param consumer       the consumer to process/processing the message
     * @param fromStatus     the previous status of the processing
     * @param toStatus       the new status of the processing
     * @param <T>            the type of the message
     * @return the new instance
     */
    public static <T> MessageProcessingUpdate<T> create(MessageDetails messageDetails,
                                                        MessageConsumer<T> consumer,
                                                        MessageProcessingStatus fromStatus,
                                                        MessageProcessingStatus toStatus) {
        return new MessageProcessingUpdate<T>(messageDetails, consumer, fromStatus, toStatus);
    }

    /**
     * Changes the state to {@link State#PROCESSING}.
     *
     * @param clock the clock used to calculate current time
     * @return an instance representing the update
     */
    public MessageProcessingUpdate<T> processing(Clock clock) {
        Is.notNull(clock, "clock must not be null");
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(PROCESSING, retries, errorMessage, now(clock), randomUuid())
        );
    }


    /**
     * Changes the state to {@link State#COMPLETED}.
     *
     * @param clock the clock used to calculate current time
     * @return an instance representing the update
     */
    public MessageProcessingUpdate<T> completed(Clock clock) {
        Is.notNull(clock, "clock must not be null");
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(COMPLETED, retries, errorMessage, now(clock), randomUuid())
        );
    }


    /**
     * Retry the processing.
     *
     * @param clock        the clock used to calculate current time
     * @param errorMessage the message of the error triggering the retry
     * @return an instance representing the update
     */
    public MessageProcessingUpdate<T> retry(Clock clock, String errorMessage) {
        Is.notNull(clock, "clock must not be null");
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(PROCESSING, retries + 1, errorMessage, now(clock), randomUuid())
        );
    }

    /**
     * Fail the processing.
     *
     * @param clock        the clock used to calculate current time
     * @param errorMessage the message of the error causing the failure
     * @return an instance representing the update
     */
    public MessageProcessingUpdate<T> failed(Clock clock, String errorMessage) {
        Is.notNull(clock, "clock must not be null");
        return new MessageProcessingUpdate<T>(
                messageDetails(),
                consumer,
                toStatus(),
                new MessageProcessingStatus(FAILED, retries, errorMessage, now(clock), randomUuid())
        );
    }

    /**
     * Gets the id of the queue the message's been published in.
     *
     * @return the queue id
     */
    public String getQueueId() {
        return queueId;
    }

    /**
     * Gets the id of the message to be/being processed.
     *
     * @return the message id
     */
    public String getMessageId() {
        return messageId;
    }

    /**
     * Gets the time the message was published to the queue.
     *
     * @return the publication time
     */
    public Date getPublicationTime() {
        return publicationTime;
    }

    /**
     * Gets the consumer to consume/consuming the message.
     *
     * @return the consumer
     */
    public MessageConsumer<T> getConsumer() {
        return consumer;
    }

    /**
     * Gets the previous state.
     *
     * @return the previous state
     */
    public MessageProcessingStatus.State getFromState() {
        return fromState;
    }

    /**
     * Gets the new state.
     *
     * @return the new state
     */
    public MessageProcessingStatus.State getToState() {
        return toState;
    }

    /**
     * Gets the number of times the consumer retried the message processing.
     *
     * @return the number of retries
     */
    public int getRetries() {
        return retries;
    }

    /**
     * Gets the message of the last error causing the processing to be retried or fail.
     *
     * @return the error message
     */
    public String getErrorMessage() {
        return errorMessage;
    }

    /**
     * Gets the time the update was made.
     *
     * @return the time the update was made
     */
    public Date getUpdateTime() {
        return updateTime;
    }

    /**
     * The previous version of the processing.
     *
     * @return the previous version
     */
    public String getFromVersionId() {
        return fromVersionId;
    }

    /**
     * The new version of the processing.
     *
     * @return the new version
     */
    public String getToVersionId() {
        return toVersionId;
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
