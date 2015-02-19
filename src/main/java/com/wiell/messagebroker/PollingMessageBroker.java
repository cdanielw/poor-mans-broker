package com.wiell.messagebroker;


public final class PollingMessageBroker implements MessageBroker {
    private final MessageQueueManager queueManager;

    // TODO: Allow lifecycle listeners to be configured - part of MessageBrokerConfig
    // One for logging, one for monitoring

    public PollingMessageBroker(MessageBrokerConfig config) {
        queueManager = new MessageQueueManager(config);
    }

    public PollingMessageBroker(MessageBrokerConfig.Builder config) {
        this(config.build());
    }

    public PollingMessageBroker start() {
        queueManager.start();
        return this;
    }

    public void stop() {
        queueManager.stop();
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType) {
        return new MessageQueue.Builder<M>(queueId, messageType, queueManager);
    }
}
