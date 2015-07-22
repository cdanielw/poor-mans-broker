package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.MessageConsumer;

import java.util.List;
import java.util.Map;

public interface MessageRepository {
    void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws MessageRepositoryException;

    void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageTakenCallback callback) throws MessageRepositoryException;

    boolean update(MessageProcessingUpdate update) throws MessageRepositoryException;

    Map<String, Integer> messageQueueSizeByConsumerId();
}
