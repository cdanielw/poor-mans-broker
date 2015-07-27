package org.openforis.rmb.slf4j;

import org.openforis.rmb.monitor.*;
import org.openforis.rmb.util.Is;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public final class Slf4jLoggingMonitor implements Monitor<Event> {
    private final Map<Class<? extends Event>, Monitor<?>> monitors =
            new HashMap<Class<? extends Event>, Monitor<?>>();

    public Slf4jLoggingMonitor() {
        add(MessageBrokerStartedEvent.class, new LoggingMonitor<MessageBrokerStartedEvent>() {
            public void onEvent(MessageBrokerStartedEvent event, Logger log) {
                log.debug("{} started",
                        event.messageBroker);
            }
        });
        add(MessageBrokerStoppedEvent.class, new LoggingMonitor<MessageBrokerStoppedEvent>() {
            public void onEvent(MessageBrokerStoppedEvent event, Logger log) {
                log.debug("{} stopped",
                        event.messageBroker);
            }
        });
        add(MessageQueueCreatedEvent.class, new LoggingMonitor<MessageQueueCreatedEvent>() {
            public void onEvent(MessageQueueCreatedEvent event, Logger log) {
                log.debug("MessageQueue {}: created with consumers {}",
                        event.queueId, event.consumers);
            }
        });
        add(CheckingForMessageQueueSizeChangesFailedEvent.class, new LoggingMonitor<CheckingForMessageQueueSizeChangesFailedEvent>() {
            public void onEvent(CheckingForMessageQueueSizeChangesFailedEvent event, Logger log) {
                log.error("Checking for message queue size changes failed. {}",
                        event.exception);
            }
        });
        add(TakingMessagesFailedEvent.class, new LoggingMonitor<TakingMessagesFailedEvent>() {
            public void onEvent(TakingMessagesFailedEvent event, Logger log) {
                log.error("Failed to take messages {}",
                        event.maxCountByConsumer, event.exception);
            }
        });
        add(MessagePublishedEvent.class, new LoggingMonitor<MessagePublishedEvent>() {
            public void onEvent(MessagePublishedEvent event, Logger log) {
                log.debug("MessageQueue {}: {} published",
                        event.queueId, event.message);
            }
        });
        add(PollingForMessagesEvent.class, new LoggingMonitor<PollingForMessagesEvent>() {
            public void onEvent(PollingForMessagesEvent event, Logger log) {
                log.trace("{} polling for messages",
                        event.maxCountByConsumer);
            }
        });
        add(ConsumingNewMessageEvent.class, new LoggingMonitor<ConsumingNewMessageEvent>() {
            public void onEvent(ConsumingNewMessageEvent event, Logger log) {
                log.debug("{} consuming new message {}",
                        event.update.getConsumer(), event.message);
            }
        });
        add(ConsumingTimedOutMessageEvent.class, new LoggingMonitor<ConsumingTimedOutMessageEvent>() {
            public void onEvent(ConsumingTimedOutMessageEvent event, Logger log) {
                log.info("{} consuming timed-out message {}",
                        event.update.getConsumer(), event.message);
            }
        });
        add(RetryingMessageConsumptionEvent.class, new LoggingMonitor<RetryingMessageConsumptionEvent>() {
            public void onEvent(RetryingMessageConsumptionEvent event, Logger log) {
                log.warn("{} retrying (#{}) to consume message {}",
                        event.update.getConsumer(), event.update.getRetries(), event.message);
            }
        });
        add(ThrottlingMessageRetryEvent.class, new LoggingMonitor<ThrottlingMessageRetryEvent>() {
            public void onEvent(ThrottlingMessageRetryEvent event, Logger log) {
                int delay = event.update.getConsumer().getThrottlingStrategy().determineDelayMillis(event.update.getRetries());
                log.debug("{} throttling {} millis before retrying (#{}) to consume message {}",
                        event.update.getConsumer(), delay, event.update.getRetries(), event.message);
            }
        });
        add(MessageConsumptionFailedEvent.class, new LoggingMonitor<MessageConsumptionFailedEvent>() {
            public void onEvent(MessageConsumptionFailedEvent event, Logger log) {
                log.error("{} failed, after {} retries, to consume message {}",
                        event.update.getConsumer(), event.update.getRetries(), event.message, event.e);
            }
        });
        add(MessageKeptAliveEvent.class, new LoggingMonitor<MessageKeptAliveEvent>() {
            public void onEvent(MessageKeptAliveEvent event, Logger log) {
                log.debug("{} sent keep-alive for {}",
                        event.update.getConsumer(), event.message);
            }
        });
        add(MessageConsumedEvent.class, new LoggingMonitor<MessageConsumedEvent>() {
            public void onEvent(MessageConsumedEvent event, Logger log) {
                log.debug("{}, after {} retries, consumed {}",
                        event.update.getConsumer(), event.update.getRetries(), event.message);
            }
        });
        add(MessageUpdateConflictEvent.class, new LoggingMonitor<MessageUpdateConflictEvent>() {
            public void onEvent(MessageUpdateConflictEvent event, Logger log) {
                log.error("{} had a message update conflict for message {}",
                        event.update.getConsumer(), event.message);
            }
        });
    }

    @SuppressWarnings("unchecked")
    public void onEvent(Event event) {
        Is.notNull(event, "event must not be null");
        Monitor monitor = monitors.get(event.getClass());
        if (monitor != null)
            monitor.onEvent(event);
    }

    private <T extends Event> void add(final Class<T> eventType, final LoggingMonitor<T> monitor) {
        monitors.put(eventType, new Monitor<T>() {
            final Logger log = LoggerFactory.getLogger(eventType);

            public void onEvent(T event) {
                monitor.onEvent(event, log);
            }
        });
    }

    private abstract static class LoggingMonitor<T extends Event> {
        protected abstract void onEvent(T event, Logger log);
    }
}
