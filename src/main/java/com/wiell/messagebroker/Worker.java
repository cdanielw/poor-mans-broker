package com.wiell.messagebroker;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
final class Worker<T> {
    private final MessageRepository repository;
    private final T message;
    private final MessageConsumer<T> consumer;
    private MessageProcessingUpdate<T> update;

    public Worker(MessageRepository repository, MessageProcessingUpdate<T> update, T message) {
        this.repository = repository;
        this.update = update;
        this.message = message;
        consumer = update.consumer;
    }

    public void consume() {
        Exception e = tryToConsume();
        if (e == null)
            return;

        while (update.retries <= consumer.maxRetries) {
            updateRepo(update.retry(e.getMessage()));
            e = tryToConsume();
            if (e == null)
                return;
        }
        updateRepo(update.failed(e.getMessage()));
    }

    private Exception tryToConsume() {
        try {
            consumer.consume(message, keepAlive());
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

    private KeepAlive keepAlive() {
        return new KeepAlive() {
            public void send() {
                updateRepo(update.processing());
            }
        };

    }
}
