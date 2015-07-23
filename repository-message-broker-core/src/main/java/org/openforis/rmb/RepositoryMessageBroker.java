package org.openforis.rmb;


import org.openforis.rmb.monitor.MessageBrokerStartedEvent;
import org.openforis.rmb.monitor.MessageBrokerStoppedEvent;
import org.openforis.rmb.util.Is;

import java.util.concurrent.atomic.AtomicBoolean;

public final class RepositoryMessageBroker implements MessageBroker {
    private final MessageBrokerConfig config;
    private final Monitors monitors;
    private final MessageQueueManager queueManager;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    public RepositoryMessageBroker(MessageBrokerConfig config) {
        Is.notNull(config, "config must not be null");
        this.config = config;
        this.monitors = config.monitors;
        this.queueManager = new MessageQueueManager(config);
    }

    public RepositoryMessageBroker(MessageBrokerConfig.Builder config) {
        Is.notNull(config, "config must not be null");
        this.config = config.build();
        this.monitors = this.config.monitors;
        this.queueManager = new MessageQueueManager(this.config);
    }

    public void start() {
        if (stopped.get())
            throw new IllegalStateException("Message broker has been stopped, and cannot be restarted");
        addShutdownHook();
        queueManager.start();
        monitors.onEvent(new MessageBrokerStartedEvent(this));
    }

    public void stop() {
        if (stopped.compareAndSet(false, true)) {
            queueManager.stop();
            monitors.onEvent(new MessageBrokerStoppedEvent(this));
        }
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId, Class<M> messageType) {
        Is.hasText(queueId, "queueId must be specified");
        Is.notNull(messageType, "messageType must not be null");
        return new MessageQueue.Builder<M>(queueId, queueManager);
    }

    public <M> MessageQueue.Builder<M> queueBuilder(String queueId) {
        Is.hasText(queueId, "queueId must be specified");
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
