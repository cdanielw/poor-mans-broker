package com.wiell.messagebroker.inmemory;

import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.spi.MessageCallback;
import com.wiell.messagebroker.spi.MessageRepository;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public final class InMemoryMessageRepository implements MessageRepository {
    private final ConcurrentHashMap<MessageConsumer<?>, ConcurrentLinkedQueue<Message>> messagesByConsumer = new ConcurrentHashMap<MessageConsumer<?>, ConcurrentLinkedQueue<Message>>();

    public void addMessage(String queueId, List<MessageConsumer<?>> consumers, String message) {
        for (MessageConsumer<?> consumer : consumers) {
            ConcurrentLinkedQueue<Message> newConsumerMessages = new ConcurrentLinkedQueue<Message>();
            ConcurrentLinkedQueue<Message> oldConsumerMessages = messagesByConsumer.putIfAbsent(consumer, newConsumerMessages);
            ConcurrentLinkedQueue<Message> consumerMessages = oldConsumerMessages == null ? newConsumerMessages : oldConsumerMessages;
            consumerMessages.add(new Message(UUID.randomUUID().toString(), message));
        }
    }

    public void takeMessages(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback) {
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            takeForConsumer(consumer, maxCount, callback);
        }
    }

    public void keepAlive(MessageConsumer<?> consumer, String messageId) {
    }

    public void completed(MessageConsumer<?> consumer, String messageId) {
    }

    public void retrying(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) {

    }

    public void failed(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) {

    }

    private void takeForConsumer(MessageConsumer<?> consumer, Integer maxCount, MessageCallback callback) {
        ConcurrentLinkedQueue<Message> messages = messagesByConsumer.get(consumer);
        if (messages == null)
            return;
        for (int i = 0; i < maxCount; i++) {
            Message message = messages.poll();
            if (message == null)
                return;
            callback.messageTaken(consumer, message.id, message.body);
        }
    }

    private static class Message {
        final String id;
        final String body;

        Message(String id, String body) {
            this.id = id;
            this.body = body;
        }
    }
}
