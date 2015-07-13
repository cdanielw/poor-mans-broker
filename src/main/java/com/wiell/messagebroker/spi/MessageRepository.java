package com.wiell.messagebroker.spi;

import com.wiell.messagebroker.MessageConsumer;

import java.util.List;
import java.util.Map;

public interface MessageRepository {
    void addMessage(String queueId, List<MessageConsumer<?>> consumers, String serializedMessage) throws MessageRepositoryException;

    void takeMessages(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback) throws MessageRepositoryException;

    void keepAlive(MessageConsumer<?> consumer, String messageId) throws MessageRepositoryException;

    void completed(MessageConsumer<?> consumer, String messageId) throws MessageRepositoryException;

    void retrying(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) throws MessageRepositoryException;

    void failed(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) throws MessageRepositoryException;

}
