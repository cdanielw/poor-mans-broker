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

    public void keepAlive(MessageConsumer<?> consumer, String messageId, String serializedMessage) {

    }

    public void consumerCompletedMessage(MessageConsumer<?> consumer, String messageId, String serializedMessage) {

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
