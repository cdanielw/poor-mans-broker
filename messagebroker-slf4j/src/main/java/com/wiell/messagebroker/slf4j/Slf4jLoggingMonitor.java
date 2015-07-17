package com.wiell.messagebroker.slf4j;

import com.wiell.messagebroker.monitor.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Slf4jLoggingMonitor implements Monitor<Event> {
    private final DispatchingMonitor dispatchingMonitor;
    private final DispatchingMonitor.Builder builder;

    public Slf4jLoggingMonitor() {
        this.builder = DispatchingMonitor.builder();
        DispatchingMonitor.Builder builder = this.builder;

        add(MessageBrokerStartedEvent.class, new LoggingMonitor<MessageBrokerStartedEvent>() {
            public void onEvent(MessageBrokerStartedEvent event, Logger log) {
                log.debug("{} started", event.messageBroker);
            }
        });
        add(MessageBrokerStoppedEvent.class, new LoggingMonitor<MessageBrokerStoppedEvent>() {
            public void onEvent(MessageBrokerStoppedEvent event, Logger log) {
                log.debug("{} stopped", event.messageBroker);
            }
        });
        add(MessageQueueCreatedEvent.class, new LoggingMonitor<MessageQueueCreatedEvent>() {
            public void onEvent(MessageQueueCreatedEvent event, Logger log) {
                log.debug("MessageQueue {}: created with consumers {}", event.queueId, event.consumers);
            }
        });
        add(ScheduledMessagePollingFailedEvent.class, new LoggingMonitor<ScheduledMessagePollingFailedEvent>() {
            public void onEvent(ScheduledMessagePollingFailedEvent event, Logger log) {
                log.error("Scheduled message polling failed", event.exception);
            }
        });
        add(MessagePublishedEvent.class, new LoggingMonitor<MessagePublishedEvent>() {
            public void onEvent(MessagePublishedEvent event, Logger log) {
                log.debug("MessageQueue {}: {} published", event.queueId, event.message);
            }
        });
        add(PollingForMessagesEvent.class, new LoggingMonitor<PollingForMessagesEvent>() {
            public void onEvent(PollingForMessagesEvent event, Logger log) {
                log.trace("{} polling for messages", event.maxCountByConsumer);
            }
        });
        add(ConsumingNewMessageEvent.class, new LoggingMonitor<ConsumingNewMessageEvent>() {
            public void onEvent(ConsumingNewMessageEvent event, Logger log) {
                log.debug("{} consuming new message {}", event.update.consumer, event.message);
            }
        });
        add(ConsumingTimedOutMessageEvent.class, new LoggingMonitor<ConsumingTimedOutMessageEvent>() {
            public void onEvent(ConsumingTimedOutMessageEvent event, Logger log) {
                log.info("{} consuming timed-out message {}", event.update.consumer, event.message);
            }
        });
        add(RetryingMessageConsumptionEvent.class, new LoggingMonitor<RetryingMessageConsumptionEvent>() {
            public void onEvent(RetryingMessageConsumptionEvent event, Logger log) {
                log.warn("{} retrying (#{}) to consume message {}", event.update.consumer, event.update.retries, event.message);
            }
        });
        add(ThrottlingMessageRetryEvent.class, new LoggingMonitor<ThrottlingMessageRetryEvent>() {
            public void onEvent(ThrottlingMessageRetryEvent event, Logger log) {
                int delay = event.update.consumer.throttlingStrategy.determineDelayMillis(event.update.retries);
                log.debug("{} throttling {} millis before retrying (#{}) to consume message {}",
                        event.update.consumer, delay, event.update.retries, event.message);
            }
        });
        add(MessageConsumptionFailedEvent.class, new LoggingMonitor<MessageConsumptionFailedEvent>() {
            public void onEvent(MessageConsumptionFailedEvent event, Logger log) {
                log.error("{} failed, after {} retries, to consume message {}", event.update.consumer, event.update.retries, event.message, event.e);
            }
        });
        add(MessageKeptAliveEvent.class, new LoggingMonitor<MessageKeptAliveEvent>() {
            public void onEvent(MessageKeptAliveEvent event, Logger log) {
                log.debug("{} sent keep-alive for {}", event.update.consumer, event.message);
            }
        });
        add(MessageConsumedEvent.class, new LoggingMonitor<MessageConsumedEvent>() {
            public void onEvent(MessageConsumedEvent event, Logger log) {
                log.debug("{}, after {} retries, consumed {}", event.update.consumer, event.update.retries, event.message);
            }
        });
        add(MessageUpdateConflictEvent.class, new LoggingMonitor<MessageUpdateConflictEvent>() {
            public void onEvent(MessageUpdateConflictEvent event, Logger log) {
                log.error("{} had a message update conflict for message {}", event.update.consumer, event.message);
            }
        });

        dispatchingMonitor = builder.build();
    }

    public void onEvent(Event event) {
        dispatchingMonitor.onEvent(event);
    }

    private <T extends Event> void add(final Class<T> eventType, final LoggingMonitor<T> monitor) {
        builder.monitor(eventType, new Monitor<T>() {
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
