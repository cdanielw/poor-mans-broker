package com.wiell.messagebroker;


import com.wiell.messagebroker.monitor.MessageBrokerStartedEvent;
import com.wiell.messagebroker.monitor.MessageBrokerStoppedEvent;
import com.wiell.messagebroker.util.Is;

import java.util.concurrent.atomic.AtomicBoolean;

public final class PollingMessageBroker implements MessageBroker {
    private final MessageBrokerConfig config;
    private final Monitors monitors;
    private final MessageQueueManager queueManager;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public PollingMessageBroker(MessageBrokerConfig config) {
        Is.notNull(config, "config must not be null");
        this.config = config;
        this.monitors = config.monitors;
        this.queueManager = new MessageQueueManager(config);
    }

    public PollingMessageBroker(MessageBrokerConfig.Builder configBuilder) {
        Is.notNull(configBuilder, "configBuilder must not be null");
        this.config = configBuilder.build();
        this.monitors = config.monitors;
        this.queueManager = new MessageQueueManager(config);
    }

    public PollingMessageBroker start() {
        if (stopped.get())
            throw new IllegalStateException("Message broker has been stopped, and cannot be restarted");
        addShutdownHook();
        queueManager.start();
        monitors.onEvent(new MessageBrokerStartedEvent(this));
        return this;
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            queueManager.stop();
            monitors.onEvent(new MessageBrokerStoppedEvent(this));
        }
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType) {
        Is.notNull(messageType, "messageType must not be null");
        return new MessageQueue.Builder<M>(queueId, queueManager);
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId) {
        Is.haveText(queueId, "queueId must be specified");
        return new MessageQueue.Builder<M>(queueId, queueManager);
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            public void run() {
                stop();
            }
        }));
    }

}
