package org.openforis.rmb;

import org.openforis.rmb.monitor.*;
import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingStatus;
import org.openforis.rmb.spi.MessageProcessingUpdate;
import org.openforis.rmb.spi.MessageRepository;

@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
final class Worker<M> {
    private final MessageRepository repository;
    private final Throttler throttler;
    private final Monitors monitors;
    private final M message;
    private final MessageConsumer<M> consumer;
    private final KeepAlive keepAlive;
    private final Clock clock = new Clock.SystemClock();

    private MessageProcessingUpdate<M> update;

    public Worker(MessageRepository repository,
                  Throttler throttler,
                  Monitors monitors,
                  MessageProcessingUpdate<M> update,
                  M message) {
        this.repository = repository;
        this.throttler = throttler;
        this.monitors = monitors;
        this.update = update;
        this.message = message;
        consumer = update.getConsumer();
        keepAlive = createKeepAlive();
    }

    public void consume() throws InterruptedException, MessageUpdateConflict {
        notifyOnWorkerStart();
        Exception e = tryToConsume();
        if (e == null)
            return;

        while (notReachedMaxRetries()) {
            retry(e);
            e = tryToConsume();
            if (e == null)
                return;
        }
        failed(e);
    }

    private synchronized boolean notReachedMaxRetries() {
        return consumer.maxRetries >= 0 && update.getRetries() < consumer.maxRetries;
    }

    private synchronized void notifyOnWorkerStart() {
        boolean timedOutMessage = update.getFromState() == MessageProcessingStatus.State.TIMED_OUT;
        if (timedOutMessage)
            monitors.onEvent(new ConsumingTimedOutMessageEvent(update, message));
        else
            monitors.onEvent(new ConsumingNewMessageEvent(update, message));
    }


    private Exception tryToConsume() {
        try {
            consumer.consume(message, keepAlive);
            completed();
            return null;
        } catch (RuntimeException e) {
            return e;
        }
    }

    // Since keep-alive can be sent from a separate thread, this.update is shared mutable state
    // All access to it has been synchronized
    private synchronized void keepAlive() {
        updateRepo(update.processing(clock));
        monitors.onEvent(new MessageKeptAliveEvent(update, message));
    }

    private synchronized void retry(Exception e) throws InterruptedException {
        updateRepo(update.retry(clock, e.getMessage()));
        monitors.onEvent(new ThrottlingMessageRetryEvent(update, message, e));
        throttler.throttle(update.getRetries(), consumer, keepAlive);
        monitors.onEvent(new RetryingMessageConsumptionEvent(update, message, e));
    }

    private synchronized void failed(Exception e) {
        updateRepo(update.failed(clock, e.getMessage()));
        monitors.onEvent(new MessageConsumptionFailedEvent(update, message, e));
    }

    private synchronized void completed() {
        updateRepo(update.completed(clock));
        monitors.onEvent(new MessageConsumedEvent(update, message));
    }

    private void updateRepo(MessageProcessingUpdate<M> update) {
        if (!repository.update(update))
            throw new MessageUpdateConflict(update);
        this.update = update;
    }

    private KeepAlive createKeepAlive() {
        return new KeepAlive() {
            public void send() { // Can be invoked from a separate thread
                keepAlive();
            }
        };
    }

    static class MessageUpdateConflict extends RuntimeException {
        final MessageProcessingUpdate<?> update;

        MessageUpdateConflict(MessageProcessingUpdate<?> update) {
            this.update = update;
        }
    }
}
