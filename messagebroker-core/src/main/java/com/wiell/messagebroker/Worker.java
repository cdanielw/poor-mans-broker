package com.wiell.messagebroker;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
final class Worker<T> {
    private final MessageRepository repository;
    private final Throttler throttler;
    private final T message;
    private final MessageConsumer<T> consumer;
    private final KeepAlive keepAlive;

    private MessageProcessingUpdate<T> update;

    public Worker(MessageRepository repository, Throttler throttler, MessageProcessingUpdate<T> update, T message) {
        this.repository = repository;
        this.throttler = throttler;
        this.update = update;
        this.message = message;
        consumer = update.consumer;
        keepAlive = createKeepAlive(this.update);
    }

    public void consume() throws InterruptedException {
        Exception e = tryToConsume();
        if (e == null)
            return;

        while (update.retries <= consumer.maxRetries) {
            updateRepo(update.retry(e.getMessage()));
            throttler.throttle(update.retries, consumer, keepAlive);
            e = tryToConsume();
            if (e == null)
                return;
        }
        updateRepo(update.failed(e.getMessage()));
    }

    private Exception tryToConsume() {
        try {
            consumer.consume(message, keepAlive);
            updateRepo(update.completed());
            return null;
        } catch (RuntimeException e) {
            // TODO: Notify someone about exception
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
            }
        };

    }
}
