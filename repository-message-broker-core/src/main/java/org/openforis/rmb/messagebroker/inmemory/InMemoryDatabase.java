package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.Clock;

import java.util.Date;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

class InMemoryDatabase {
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock read = readWriteLock.readLock();
    private final Lock write = readWriteLock.writeLock();

    private final Map<MessageConsumer<?>, ConsumerMessages> consumerMessagesByConsumer =
            new ConcurrentHashMap<MessageConsumer<?>, ConsumerMessages>();

    <T> T read(MessageConsumer<?> consumer, Operation<T> operation) {
        read.lock();
        try {
            return operation.execute(consumerMessages(consumer));
        } finally {
            read.unlock();
        }
    }

    <T> T write(MessageConsumer<?> consumer, Operation<T> operation) {
        write.lock();
        try {
            return operation.execute(consumerMessages(consumer));
        } finally {
            write.unlock();
        }
    }

    private ConsumerMessages consumerMessages(MessageConsumer<?> consumer) {
        if (!consumerMessagesByConsumer.containsKey(consumer))
            consumerMessagesByConsumer.put(consumer, new ConsumerMessages(consumer));
        return consumerMessagesByConsumer.get(consumer);
    }

    static abstract class Operation<T> {
        final Clock clock;

        public Operation(Clock clock) {
            this.clock = clock;
        }

        abstract T execute(ConsumerMessages consumerMessages);

        final String randomUuid() {
            return UUID.randomUUID().toString();
        }

        final Date now() {
            return new Date(clock.millis());
        }
    }
}
