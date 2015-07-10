package com.wiell.messagebroker.jdbc;

import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.MessageRepository;

import java.util.List;
import java.util.Map;

public final class JdbcMessageRepository implements MessageRepository {
    public void addMessage(String queueId, List<MessageConsumer<?>> consumers, String serializedMessage) {

    }

    public void takeMessages(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback) {

    }

    public void keepAlive(MessageConsumer<?> consumer, String messageId) {

    }

    public void completed(MessageConsumer<?> consumer, String messageId) {

    }

    public void retrying(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) {

    }

    public void failed(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) {

    }
    /*

 -----------------
| message_processing
|-----------------
| message_id
| consumer_id
| version
| last_updated
|


*/
}
