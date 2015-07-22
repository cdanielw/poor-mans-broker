package org.openforis.rmb.messagebroker;

import org.openforis.rmb.messagebroker.monitor.PollingForMessageQueueSizeChangesFailedEvent;
import org.openforis.rmb.messagebroker.monitor.PollingForTimedOutMessagesFailedEvent;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

class MessageRepositoryWatcher {
    private final MessagePoller messagePoller;
    private final MessageQueueSizeChecker queueSizeChecker;
    private final Monitors monitors;
    private final long pollingPeriod;
    private final TimeUnit pollingTimeUnit;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            NamedThreadFactory.singleThreadFactory("rmb.MessageRepositoryWatcher")
    );

    MessageRepositoryWatcher(MessagePoller messagePoller, MessageBrokerConfig config) {
        this.messagePoller = messagePoller;
        this.monitors = config.monitors;
        this.queueSizeChecker = new MessageQueueSizeChecker(config.messageRepository, monitors);
        this.pollingPeriod = config.repositoryWatcherPollingPeriod;
        this.pollingTimeUnit = config.repositoryWatcherPollingTimeUnit;
    }

    void includeQueue(String queueId, List<MessageConsumer<?>> consumers) {
        queueSizeChecker.includeQueue(queueId, consumers);
    }


    void start() {
        executor.scheduleWithFixedDelay(new Runnable() {
            public void run() {
                pollForTimedOutMessages();
                pollForQueueSizeUpdates();
            }
        }, 0, pollingPeriod, pollingTimeUnit);
    }

    private void pollForQueueSizeUpdates() {
        try {
            queueSizeChecker.check();
        } catch (Exception e) {
            monitors.onEvent(new PollingForMessageQueueSizeChangesFailedEvent(e));
        }
    }

    private void pollForTimedOutMessages() {
        try {
            messagePoller.poll();
        } catch (Exception e) {
            monitors.onEvent(new PollingForTimedOutMessagesFailedEvent(e));
        }
    }

    void stop() {
        ExecutorTerminator.shutdownAndAwaitTermination(executor);
    }
}
