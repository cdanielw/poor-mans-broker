package com.wiell.messagebroker.inmemory;

import com.wiell.messagebroker.*;
import com.wiell.messagebroker.util.Clock;

import java.util.*;

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.*;

public final class InMemoryMessageRepository implements MessageRepository {
    private final Object lock = new Object();
    private final Map<MessageConsumer<?>, ConsumerMessages> consumerMessagesByConsumer =
            new HashMap<MessageConsumer<?>, ConsumerMessages>();
    private Clock clock = new Clock.SystemClock();

    void setClock(Clock clock) {
        this.clock = clock;
    }

    public void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) {
        synchronized (lock) {
            for (MessageConsumer<?> consumer : consumers) {
                ConsumerMessages consumerMessages = consumerMessages(consumer);
                String messageId = UUID.randomUUID().toString();
                MessageProcessingUpdate<?> update = MessageProcessingUpdate.create(consumer, messageId, PENDING, PENDING, 0, null, null);
                consumerMessages.add(new Message(update, serializedMessage));
            }
        }
    }

    public void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback) {
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            takeForConsumer(consumer, maxCount, callback);
        }
    }

    private void takeForConsumer(MessageConsumer<?> consumer, Integer maxCount, MessageCallback callback) {
        ConsumerMessages messages = consumerMessages(consumer);
        if (messages == null)
            return;
        for (int i = 0; i < maxCount; i++) {
            Message message;
            synchronized (lock) {
                message = messages.takePending();
            }
            if (message == null)
                return;
            callback.messageTaken(message.update, message.serializedMessage);
        }
    }

    private ConsumerMessages consumerMessages(MessageConsumer<?> consumer) {
        ConsumerMessages messages = consumerMessagesByConsumer.get(consumer);
        if (messages == null) {
            messages = new ConsumerMessages();
            consumerMessagesByConsumer.put(consumer, messages);
        }
        return messages;
    }

    public boolean update(MessageProcessingUpdate update) throws MessageRepositoryException {
        synchronized (lock) {
            ConsumerMessages messages = consumerMessages(update.consumer);
            Message message = messages.find(update.messageId);
            messages.update(message, update);
            return true;
        }
    }

    private static class ConsumerMessages {
        Set<Message> messages = new LinkedHashSet<Message>();
        Map<String, Message> messageById = new HashMap<String, Message>();

        void add(Message message) {
            messages.add(message);
            messageById.put(message.update.messageId, message);
        }

        Message find(String messageId) {
            return messageById.get(messageId);
        }

        void update(Message message, MessageProcessingUpdate update) {
            message.setUpdate(update);
            if (message.isCompleted()) {
                messageById.remove(message.update.messageId);
                messages.remove(message);
            }
        }

        public Message takePending() {
            for (Message message : messages) {
                if (message.isPending() || message.timedOut()) {
                    message.processing();
                    return message;
                }
            }
            return null;
        }
    }

    private class Message {
        MessageProcessingUpdate update;
        Date timesOut;
        final Object serializedMessage;

        Message(MessageProcessingUpdate update, Object serializedMessage) {
            this.update = update;
            this.timesOut = new Date(clock.millis() + update.consumer.timeUnit.toMillis(update.consumer.timeout));
            this.serializedMessage = serializedMessage;
        }

        void processing() {
            setUpdate(update.processing());
        }

        void setUpdate(MessageProcessingUpdate update) {
            this.update = update;
        }

        boolean isPending() {
            return update.toStatus == PENDING;
        }

        boolean timedOut() {
            return update.toStatus == PROCESSING && timesOut.before(new Date());
        }

        boolean isCompleted() {
            return update.toStatus == COMPLETED;
        }
    }
}
