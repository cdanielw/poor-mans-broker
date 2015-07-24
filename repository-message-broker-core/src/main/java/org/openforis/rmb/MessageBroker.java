package org.openforis.rmb;

/**
 * A message broker, responsible for the creation and management of {@link MessageQueue}s.
 */
public interface MessageBroker {

    /**
     * Starts the message broker. This method must be invoked before publishing any message to a queue.
     * This will fail if the broker already is started or has been stopped.
     * <p>
     * In addition to starting the broker, a shutdown hook will also be registered.
     * </p>
     */
    void start();

    /**
     * Stops the message broker. This is a no-op if the broker never been started or already been stopped.
     */
    void stop();

    /**
     * Provides a builder for creating {@link MessageQueue}s.
     *
     * @param queueId     the id of the queue to build. Must not be null and must be unique within the message broker.
     * @param messageType the type of messages to be published to the queue. Must not be null.
     * @param <M>         the type of messages to be published to the queue
     * @return the queue builder
     */
    <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType);

    /**
     * Provides a builder for creating {@link MessageQueue}s.
     *
     * @param queueId the id of the queue to build. Must not be null and must be unique within the message broker.
     * @param <M>     the type of messages to be published to the queue
     * @return the queue builder
     */
    <M> MessageQueue.Builder<M> queueBuilder(String queueId);
}
