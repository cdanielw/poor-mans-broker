package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageProcessingFilter;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;
import org.openforis.rmb.messagebroker.spi.MessageRepository;
import org.openforis.rmb.messagebroker.util.Is;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class InMemoryMessageRepository implements MessageRepository {
    private Clock clock = new Clock.SystemClock();
    private InMemoryDatabase database = new InMemoryDatabase();

    void setClock(Clock clock) {
        Is.notNull(clock, "clock must not be null");
        this.clock = clock;
    }

    public void add(
            String queueId,
            List<MessageConsumer<?>> consumers,
            Object serializedMessage
    ) {
        Is.hasText(queueId, "queueId must be specified");
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(serializedMessage, "serializedMessage must not be null");
        for (MessageConsumer<?> consumer : consumers) {
            database.write(consumer, new AddMessage(clock, queueId, serializedMessage));
        }
    }

    public void take(
            Map<MessageConsumer<?>, Integer> maxCountByConsumer,
            MessageTakenCallback callback
    ) {
        Is.notEmpty(maxCountByConsumer, "maxCountByConsumer must not be empty");
        Is.notNull(callback, "callback must not be null");
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            database.write(consumer, new TakeMessages(clock, maxCount, callback));
        }
    }

    public boolean update(
            MessageProcessingUpdate update
    ) {
        Is.notNull(update, "update must not be null");
        return database.write(update.getConsumer(), new UpdateMessageProcessing(clock, update));
    }

    public void findMessageProcessing(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter,
            MessageProcessingFoundCallback callback
    ) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(filter, "filter must not be null");
        Is.notNull(callback, "callback must not be null");
        for (MessageConsumer<?> consumer : consumers)
            database.read(consumer, new FindMessageProcessing(clock, filter, callback));
    }

    public Map<MessageConsumer<?>, Integer> messageCountByConsumer(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter
    ) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(filter, "filter must not be null");
        Map<MessageConsumer<?>, Integer> countByConsumer = new HashMap<MessageConsumer<?>, Integer>();
        for (MessageConsumer<?> consumer : consumers)
            countByConsumer.put(consumer, database.read(consumer, new CountMessages(clock, filter)));
        return countByConsumer;
    }

    public void deleteMessageProcessing(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter
    ) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(filter, "filter must not be null");
        for (MessageConsumer<?> consumer : consumers)
            database.write(consumer, new DeleteMessageProcessing(clock, filter));
    }

}
