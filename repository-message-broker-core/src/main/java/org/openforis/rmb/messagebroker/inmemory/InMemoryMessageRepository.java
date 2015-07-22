package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.*;

import java.util.*;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*;

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
                long publicationTime = clock.millis();
                MessageProcessingUpdate<?> update = MessageProcessingUpdate
                        .createNew(consumer, new MessageDetails(queueId, messageId, publicationTime));
                consumerMessages.add(new Message(update, serializedMessage));
            }
        }
    }

    public void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageTakenCallback callback) {
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            takeForConsumer(consumer, maxCount, callback);
        }
    }

    public Map<String, Integer> messageQueueSizeByConsumerId() {
        Map<String, Integer> sizeByConsumerId = new HashMap<String, Integer>();
        for (Map.Entry<MessageConsumer<?>, ConsumerMessages> entry : consumerMessagesByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            ConsumerMessages messages = entry.getValue();
            sizeByConsumerId.put(consumer.id, messages.queueSize());
        }
        return sizeByConsumerId;
    }

    private void takeForConsumer(MessageConsumer<?> consumer, Integer maxCount, MessageTakenCallback callback) {
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

        Message takePending() {
            for (Message message : messages) {
                if (message.isPending() || message.timedOut()) {
                    message.processing();
                    return message;
                }
            }
            return null;
        }

        int queueSize() {
            int timedOutCount = 0;
            int i = 0;
            for (Message message : messages) {
                if (message.isPending()) // When a message is pending means the rest if the messages are too
                    return messages.size() - i + timedOutCount;
                if (message.timedOut())
                    timedOutCount++;
                i++;
            }
            return timedOutCount;
        }
    }

    private class Message {
        MessageProcessingUpdate update;
        Date timesOut;
        final Object serializedMessage;

        Message(MessageProcessingUpdate update, Object serializedMessage) {
            this.update = update;
            this.timesOut = new Date(update.publicationTime + update.consumer.timeUnit.toMillis(update.consumer.timeout));
            this.serializedMessage = serializedMessage;
        }

        void processing() {
            MessageProcessingStatus.State fromState = timedOut() ? TIMED_OUT : update.toState;
            setUpdate(MessageProcessingUpdate.take(
                    update.consumer,
                    new MessageDetails(update.queueId, update.messageId, update.publicationTime),
                    new MessageProcessingStatus(fromState, update.retries, update.errorMessage, update.toVersionId)
            ));
        }

        void setUpdate(MessageProcessingUpdate update) {
            this.update = update;
        }

        boolean isPending() {
            return update.toState == PENDING;
        }

        boolean timedOut() {
            return update.toState == PROCESSING && timesOut.before(new Date());
        }

        boolean isCompleted() {
            return update.toState == COMPLETED;
        }
    }
}
