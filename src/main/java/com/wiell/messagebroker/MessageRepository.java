package com.wiell.messagebroker;

import java.util.List;
import java.util.Map;

public interface MessageRepository {
    void addMessage(String queueId, List<MessageConsumer<?>> consumers, String serializedMessage);

    void takeMessages(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback);

    void keepAlive(MessageConsumer<?> consumer, String messageId, String serializedMessage);

    void consumerCompletedMessage(MessageConsumer<?> consumer, String messageId, String serializedMessage);

//    void failed(MessageProcessingJob job, int retries, Exception exception);


    interface MessageCallback {
        <T> void messageTaken(MessageConsumer<T> consumer, String messageId, String serializedMessage);
    }
}
