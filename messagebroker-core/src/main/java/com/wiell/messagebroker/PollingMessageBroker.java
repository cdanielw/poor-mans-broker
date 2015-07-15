package com.wiell.messagebroker;


public final class PollingMessageBroker implements MessageBroker {
    private final MessageQueueManager queueManager;

    public PollingMessageBroker(MessageBrokerConfig config) {
        Is.notNull(config, "config must not be null");
        queueManager = new MessageQueueManager(config);
    }

    public PollingMessageBroker(MessageBrokerConfig.Builder config) {
        Is.notNull(config, "config must not be null");
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
        Is.haveText(queueId, "queueId must be specified");
        Is.notNull(messageType, "messageType must not be null");
        return new MessageQueue.Builder<M>(queueId, messageType, queueManager);
    }
}
