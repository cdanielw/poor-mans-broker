package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.*;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
final class Worker<T> {
    private final MessageRepository repository;
    private final Throttler throttler;
    private final Monitors monitors;
    private final T message;
    private final MessageConsumer<T> consumer;
    private final KeepAlive keepAlive;

    private MessageProcessingUpdate<T> update;

    public Worker(MessageRepository repository,
                  Throttler throttler,
                  Monitors monitors,
                  MessageProcessingUpdate<T> update,
                  T message) {
        this.repository = repository;
        this.throttler = throttler;
        this.monitors = monitors;
        this.update = update;
        this.message = message;
        consumer = update.consumer;
        keepAlive = createKeepAlive(this.update);
    }

    public void consume() throws InterruptedException {
        if (timedOutMessage())
            monitors.onEvent(new ConsumingTimedOutMessageEvent<T>(update, message));
        else
            monitors.onEvent(new ConsumingNewMessageEvent<T>(update, message));
        Exception e = tryToConsume();
        if (e == null)
            return;

        while (update.retries <= consumer.maxRetries) {
            updateRepo(update.retry(e.getMessage()));
            throttler.throttle(update.retries, consumer, keepAlive);
            monitors.onEvent(new RetryingMessageConsumptionEvent<T>(update, message, e));
            e = tryToConsume();
            if (e == null)
                return;
        }
        updateRepo(update.failed(e.getMessage()));
        monitors.onEvent(new MessageConsumptionFailedEvent<T>(update, message, e));
    }

    private boolean timedOutMessage() {
        return update.fromStatus == MessageProcessingUpdate.Status.PROCESSING;
    }

    private Exception tryToConsume() {
        try {
            consumer.consume(message, keepAlive);
            updateRepo(update.completed());
            monitors.onEvent(new MessageConsumedEvent<T>(update, message));
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    private synchronized void updateRepo(MessageProcessingUpdate<T> update) {
        repository.update(update);
        this.update = update;
    }

    private KeepAlive createKeepAlive(final MessageProcessingUpdate<T> update) {
        return new KeepAlive() {
            public void send() {
                updateRepo(update.processing());
                monitors.onEvent(new MessageKeptAliveEvent<T>(update, message));
            }
        };
    }
}
