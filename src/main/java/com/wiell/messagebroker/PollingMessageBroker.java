package com.wiell.messagebroker;


public final class PollingMessageBroker implements MessageBroker {
    private final MessageQueueManager queueManager;

    // TODO: Allow lifecycle listeners to be configured - part of MessageBrokerConfig
    // One for logging, one for monitoring

    public PollingMessageBroker(MessageBrokerConfig config) {
        Check.notNull(config, "config must not be null");
        queueManager = new MessageQueueManager(config);
    }

    public PollingMessageBroker(MessageBrokerConfig.Builder config) {
        Check.notNull(config, "config must not be null");
        queueManager = new MessageQueueManager(config.build());
    }

    public PollingMessageBroker start() {
        queueManager.start();
        return this;
    }

    public void stop() {
        queueManager.stop();
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType) {
        Check.haveText(queueId, "queueId must be specified");
        Check.notNull(messageType, "messageType must not be null");
        return new MessageQueue.Builder<M>(queueId, messageType, queueManager);
    }
}
