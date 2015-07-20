package org.openforis.rmb.messagebroker;

import java.util.List;
import java.util.Map;

public interface MessageRepository {
    void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws MessageRepositoryException;

    void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback) throws MessageRepositoryException;

    boolean update(MessageProcessingUpdate update) throws MessageRepositoryException;

    Map<String, Integer> messageQueueSizeByConsumerId();
}
