package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.*;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class InMemoryMessageRepository implements MessageRepository {
    private Clock clock = new Clock.SystemClock();
    private InMemoryDatabase database = new InMemoryDatabase();

    void setClock(Clock clock) {
        this.clock = clock;
    }

    public void add(
            String queueId, List<MessageConsumer<?>> consumers,
            Object serializedMessage
    ) throws MessageRepositoryException {
        for (MessageConsumer<?> consumer : consumers) {
            database.write(consumer, new AddMessage(clock, queueId, serializedMessage));
        }
    }

    public void take(
            Map<MessageConsumer<?>, Integer> maxCountByConsumer,
            MessageTakenCallback callback
    ) throws MessageRepositoryException {
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            database.write(consumer, new TakeMessages(clock, maxCount, callback));
        }
    }

    public boolean update(
            MessageProcessingUpdate update
    ) throws MessageRepositoryException {
        return database.write(update.consumer, new UpdateMessageProcessing(clock, update));
    }

    public void findMessageProcessing(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter,
            MessageProcessingFoundCallback callback
    ) throws MessageRepositoryException {
        for (MessageConsumer<?> consumer : consumers)
            database.read(consumer, new FindMessageProcessing(clock, filter, callback));
    }

    public Map<MessageConsumer<?>, Integer> messageCountByConsumer(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter
    ) throws MessageRepositoryException {
        Map<MessageConsumer<?>, Integer> countByConsumer = new HashMap<MessageConsumer<?>, Integer>();
        for (MessageConsumer<?> consumer : consumers)
            countByConsumer.put(consumer, database.read(consumer, new CountMessages(clock, filter)));
        return countByConsumer;
    }

    public void deleteMessageProcessing(
            Collection<MessageConsumer<?>> consumers,
            MessageProcessingFilter filter
    ) throws MessageRepositoryException {
        for (MessageConsumer<?> consumer : consumers)
            database.write(consumer, new DeleteMessageProcessing(clock, filter));
    }

}
