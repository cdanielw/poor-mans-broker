package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.MessageConsumer;

import java.util.List;
import java.util.Map;

public interface MessageRepository {
    void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws MessageRepositoryException;

    void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageTakenCallback callback) throws MessageRepositoryException;

    boolean update(MessageProcessingUpdate update) throws MessageRepositoryException;

    // TODO: Delete this, and use generic count
    Map<String, Integer> messageQueueSizeByConsumerId();

//    void findMessageProcessing(MessageProcessingFilter filter, MessageProcessingCallback callback);
//
//    Map<String, Integer> countByConsumerId(MessageProcessingFilter filter);
//
//    void deleteMessageProcessing(MessageProcessingFilter filter);
}
