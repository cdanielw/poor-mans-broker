package org.openforis.rmb;


import org.openforis.rmb.spi.TransactionSynchronizer;
import org.openforis.rmb.util.Is;

import java.util.ArrayList;
import java.util.List;

/**
 * Publishes messages to a backing repository. Messages can only be published in a transaction,
 * and an exception is thrown if it's not.
 * <p>
 * Once a message's been published and the transaction commits, the message will be delivered to available consumers,
 * if any.
 * </p>
 * <p>
 * An effort to prevent duplicate messages is made, but it cannot be guaranteed.
 * Therefore it's strongly suggested to make message handlers idempotent idempotent.
 * Message ordering is guaranteed by the provided JDBC backed message repository.
 * </p>
 * <p>
 * Instances of this class are created through a builder: {@link MessageBroker#queueBuilder(String)} or
 * {@link MessageBroker#queueBuilder(String, Class)}.
 * </p>
 *
 * @param <M> the type of messages to be published to the queue
 */
public interface MessageQueue<M> {
    /**
     * Publish message to a backing repository. This can only be published in a transaction,
     * and an exception is thrown if it's not.
     * <p>
     * Once the message's been published and the transaction commits,
     * the message will be delivered to any available consumer.
     * </p>
     * The, on the {@link RepositoryMessageBroker} configured, {@link TransactionSynchronizer}
     * is used to determine if message is published in a transaction or not, and notifies when the transaction commits.
     *
     * @param message the message to publish
     */
    void publish(M message);

    /**
     * Builds {@link MessageQueue} instances. Register consumers the {@link MessageQueue} by calling
     * {@link #consumer(MessageConsumer.Builder)} or
     * {@link #consumer(MessageConsumer)}, then finally call {@link #build()}.
     * <p>
     * Instances of this class are created though
     * {@link RepositoryMessageBroker#queueBuilder(String)} or
     * {@link RepositoryMessageBroker#queueBuilder(String, Class)}.
     * </p>
     *
     * @param <M> the type of messages to be published to the queue
     */
    final class Builder<M> {
        private String queueId;
        private MessageQueueManager queueManager;
        private List<MessageConsumer<M>> consumers = new ArrayList<MessageConsumer<M>>();

        @SuppressWarnings("UnusedParameters")
        Builder(String queueId, MessageQueueManager queueManager) {
            this.queueId = queueId;
            this.queueManager = queueManager;
        }

        /**
         * Register a message queue consumer.
         * <p>The consumers are in turn created through a builder from either
         * {@link MessageConsumer#builder(String, MessageHandler)} or
         * {@link MessageConsumer#builder(String, KeepAliveMessageHandler)}
         * </p>
         *
         * @param consumer the consumer to register
         * @return the builder, so methods can be chained
         */
        public Builder<M> consumer(MessageConsumer<M> consumer) {
            Is.notNull(consumer, "consumer must not be null");
            consumers.add(consumer);
            return this;
        }

        /**
         * Register a message queue consumer. This overloaded method is a short-cut that takes a consumer builder,
         * instead of the consumer itself.
         * <p>
         * The consumer builders are in turn created through either
         * {@link MessageConsumer#builder(String, MessageHandler)} or
         * {@link MessageConsumer#builder(String, KeepAliveMessageHandler)}
         * </p>
         *
         * @param consumer the consumer to register
         * @return the builder, so methods can be chained
         */
        public Builder<M> consumer(MessageConsumer.Builder<M> consumer) {
            Is.notNull(consumer, "consumer must not be null");
            consumers.add(consumer.build());
            return this;
        }

        /**
         * Builds the {@link MessageQueue}, based on how the builder's been configured.
         *
         * @return the message queue instance
         */
        @SuppressWarnings("unchecked")
        public MessageQueue<M> build() {
            Default<M> queue = new Default<M>(this);
            List uncheckedConsumers = consumers;
            queueManager.registerQueue(queueId, uncheckedConsumers);
            return queue;
        }

        private static class Default<M> implements MessageQueue<M> {
            private final String id;
            private final MessageQueueManager queueManager;

            private Default(Builder<M> builder) {
                id = builder.queueId;
                queueManager = builder.queueManager;
            }

            public void publish(M message) {
                Is.notNull(message, "message must not be null");
                queueManager.publish(id, message);
            }
        }
    }
}
