package org.openforis.rmb.spi;

import org.openforis.rmb.MessageConsumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Responsible for interacting with messages in some data repository.
 */
public interface MessageRepository {
    /**
     * Add a message to a queue.
     *
     * @param queueId           the queue to add the message to
     * @param consumers         the consumers to handle the message
     * @param serializedMessage the message, in serialized form. It should either be a String or a byte[]
     */
    void add(
            String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage
    ) throws MessageRepositoryException;

    /**
     * Take messages for consumers to process.
     *
     * @param maxCountByConsumer the max number of messages to take, by consumer
     * @param callback           to be invoked when taking a message
     */
    void take(
            Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageTakenCallback callback
    ) throws MessageRepositoryException;

    /**
     * Update the status of a message processing
     *
     * @param update the update to make
     * @return false if there was a conflict when updating the status
     */
    boolean update(
            MessageProcessingUpdate update
    ) throws MessageRepositoryException;

    /**
     * Find message processing for the provided consumers, filtered by provided filter.
     *
     * @param consumers the consumers to search message processing for
     * @param filter    specification which message processing to include/exclude
     * @param callback  to be invoked when a message processing is found
     */
    void findMessageProcessing(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter,
            MessageProcessingFoundCallback callback
    ) throws MessageRepositoryException;

    /**
     * Determine the number of messages for the provided consumers, filtered by provided filter.
     *
     * @param consumers the consumers to get the count for
     * @param filter    specification which message processing to include/exclude
     * @return the message processing count, by consumer
     */
    Map<MessageConsumer<?>, Integer> messageCountByConsumer(
            Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter
    ) throws MessageRepositoryException;

    /**
     * Delete message processing for the provided consumers, matching the provided filter.
     *
     * @param consumers the consumers to delete message processing for
     * @param filter    specification which message processing to delete
     */
    void deleteMessageProcessing(
            Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter
    ) throws MessageRepositoryException;

    /**
     * Callback when taking messages.
     */
    interface MessageTakenCallback {
        /**
         * Invoked when message processing has been taken.
         *
         * @param update            the update made when taking the message
         * @param serializedMessage the serialized message, which will either be a String or a byte[]
         */
        void taken(MessageProcessingUpdate update, Object serializedMessage);
    }

    /**
     * Callback when finding message processing.
     */
    interface MessageProcessingFoundCallback {
        /**
         * Invoked when message processing has been found.
         *
         * @param messageProcessing the message processing found
         * @param serializedMessage the serialized message, which will either be a String or a byte[]
         */
        void found(MessageProcessing messageProcessing, Object serializedMessage);
    }
}
