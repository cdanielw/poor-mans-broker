package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.MessageConsumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface MessageRepository {
    void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws MessageRepositoryException;

    void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageTakenCallback callback) throws MessageRepositoryException;

    boolean update(MessageProcessingUpdate update) throws MessageRepositoryException;

    void findMessageProcessing(Collection<MessageConsumer<?>> consumers,
                               MessageProcessingFilter filter,
                               MessageProcessingFoundCallback callback);

    Map<MessageConsumer<?>, Integer> messageCountByConsumer(Collection<MessageConsumer<?>> consumers,
                                                            MessageProcessingFilter filter);
//
//    void deleteMessageProcessing(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter);


    interface MessageTakenCallback {
        void taken(MessageProcessingUpdate update, Object serializedMessage);
    }

    interface MessageProcessingFoundCallback {
        void found(MessageProcessing messageProcessing, Object serializedMessage);
    }
}
