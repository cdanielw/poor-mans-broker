package com.wiell.messagebroker;

final class Worker<T> {
    private final MessageRepository repository;

    public Worker(MessageRepository repository) {
        this.repository = repository;
    }

    public void consume(MessageConsumer<T> consumer, String messageId, T message) {
        int retry = 0;
        Exception e = tryToConsume(consumer, messageId, message, retry);
        if (e == null)
            return;

        while (retry <= consumer.maxRetries) {
            retry++;
            repository.retrying(consumer, messageId, retry, e);
            e = tryToConsume(consumer, messageId, message, retry);
            if (e == null)
                return;
        }
        repository.failed(consumer, messageId, retry, e);
    }

    private Exception tryToConsume(MessageConsumer<T> consumer, String messageId, T message, int retry) {
        try {
            consumer.consume(message, keepAlive(consumer, messageId));
            repository.completed(consumer, messageId);
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    private KeepAlive keepAlive(final MessageConsumer<?> consumer, final String messageId) {
        return new KeepAlive() {
            public void send() {
                repository.keepAlive(consumer, messageId);
            }
        };

    }
}
