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
                String messageId = randomUuid();
                Date publicationTime = now();
                MessageProcessingUpdate<?> update = MessageProcessingUpdate
                        .create(
                                new MessageDetails(queueId, messageId, publicationTime),
                                consumer,
                                new MessageProcessingStatus(PENDING, 0, null, now(), randomUuid()),
                                new MessageProcessingStatus(PENDING, 0, null, now(), randomUuid())
                        );
                consumerMessages.add(new Message(update, serializedMessage));
            }
        }
    }

    private String randomUuid() {
        return UUID.randomUUID().toString();
    }

    public void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageTakenCallback callback) {
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            takeForConsumer(consumer, maxCount, callback);
        }
    }

    public boolean update(MessageProcessingUpdate update) throws MessageRepositoryException {
        synchronized (lock) {
            ConsumerMessages messages = consumerMessages(update.consumer);
            Message message = messages.find(update.messageId);
            messages.update(message, update);
            return true;
        }
    }

    public void findMessageProcessing(Collection<MessageConsumer<?>> consumers,
                                      MessageProcessingFilter filter,
                                      MessageProcessingFoundCallback callback) {
        for (MessageConsumer<?> consumer : consumers) {
            if (consumerMessagesByConsumer.containsKey(consumer))
                findMessageProcessing(consumer, filter, callback);
        }
    }

    public Map<MessageConsumer<?>, Integer> messageCountByConsumer(Collection<MessageConsumer<?>> consumers,
                                                                   MessageProcessingFilter filter) {
        Map<MessageConsumer<?>, Integer> countByConsumer = new HashMap<MessageConsumer<?>, Integer>();
        for (MessageConsumer<?> consumer : consumers)
            countByConsumer.put(consumer, findMessagesForConsumer(consumer, filter).size());
        return countByConsumer;
    }

    public void deleteMessageProcessing(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter) throws MessageRepositoryException {
        for (MessageConsumer<?> consumer : consumers) {
            ConsumerMessages consumerMessages = consumerMessagesByConsumer.get(consumer);
            for (Message message : findMessagesForConsumer(consumer, filter))
                consumerMessages.remove(message);
        }
    }

    private void findMessageProcessing(MessageConsumer<?> consumer,
                                       MessageProcessingFilter filter,
                                       MessageProcessingFoundCallback callback) {
        List<Message> messages = findMessagesForConsumer(consumer, filter);
        for (Message message : messages) {
            MessageProcessingUpdate update = message.update;
            callback.found(MessageProcessing.create(
                    new MessageDetails(update.queueId, update.messageId, update.publicationTime),
                    consumer,
                    new MessageProcessingStatus(update.toState, update.retries, update.errorMessage, now(), update.toVersionId)
            ), message.serializedMessage);
        }
    }

    private List<Message> findMessagesForConsumer(MessageConsumer<?> consumer, MessageProcessingFilter filter) {
        ConsumerMessages consumerMessages = consumerMessagesByConsumer.get(consumer);
        if (consumerMessages == null)
            return Collections.emptyList();
        List<Message> messages = new ArrayList<Message>();
        for (Message message : consumerMessages.messages)
            if (include(message, filter))
                messages.add(message);
        return messages;
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean include(Message message, MessageProcessingFilter filter) {
        MessageProcessingUpdate update = message.update;
        if (!filter.states.isEmpty()) {
            boolean notMatchingState = !filter.states.contains(update.toState);
            boolean notMatchingTimedOut = !filter.states.contains(TIMED_OUT) || !message.timedOut();
            if (notMatchingState && notMatchingTimedOut)
                return false;
        }
        if (filter.publishedBefore != null && !update.publicationTime.before(filter.publishedBefore))
            return false;
        if (filter.publishedAfter != null && !update.publicationTime.after(filter.publishedAfter))
            return false;
        if (filter.lastUpdatedBefore != null && !update.updateTime.before(filter.lastUpdatedBefore))
            return false;
        if (filter.lastUpdatedAfter != null && !update.updateTime.after(filter.lastUpdatedAfter))
            return false;
        if (!filter.messageIds.isEmpty() && !filter.messageIds.contains(message.update.messageId))
            return false;

        return true;
    }

    private void takeForConsumer(MessageConsumer<?> consumer, Integer maxCount, MessageTakenCallback callback) {
        ConsumerMessages messages = consumerMessages(consumer);
        if (messages == null)
            return;
        messages.takePending(maxCount, callback);
    }

    private ConsumerMessages consumerMessages(MessageConsumer<?> consumer) {
        ConsumerMessages messages = consumerMessagesByConsumer.get(consumer);
        if (messages == null) {
            messages = new ConsumerMessages();
            consumerMessagesByConsumer.put(consumer, messages);
        }
        return messages;
    }


    private Date now() {
        return new Date(clock.millis());
    }


    private class ConsumerMessages {
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
            if (message.isCompleted())
                remove(message);
        }

        private void remove(Message message) {
            messageById.remove(message.update.messageId);
            messages.remove(message);
        }

        void takePending(Integer maxCount, MessageTakenCallback callback) {
            int i = 0;
            for (Message message : messages) {
                if (i >= maxCount) return;
                boolean taken = false;
                synchronized (lock) {
                    if (message.isPending() || message.timedOut()) {
                        message.take();
                        taken = true;
                    }
                }
                if (taken)
                    callback.taken(message.update, message.serializedMessage);
                i++;
            }
        }
    }


    private class Message {
        MessageProcessingUpdate update;
        Date timesOut;
        final Object serializedMessage;

        Message(MessageProcessingUpdate update, Object serializedMessage) {
            this.update = update;
            long timeoutMillis = update.consumer.timeUnit.toMillis(update.consumer.timeout);
            this.timesOut = new Date(update.publicationTime.getTime() + timeoutMillis);
            this.serializedMessage = serializedMessage;
        }

        void take() {
            MessageProcessingStatus.State fromState = timedOut() ? TIMED_OUT : update.toState;
            setUpdate(MessageProcessingUpdate.create(
                    new MessageDetails(update.queueId, update.messageId, update.publicationTime), update.consumer,
                    new MessageProcessingStatus(fromState, update.retries, update.errorMessage, now(), update.toVersionId),
                    new MessageProcessingStatus(PROCESSING, update.retries, update.errorMessage, now(), randomUuid())
            ));
        }

        void setUpdate(MessageProcessingUpdate update) {
            this.update = update;
        }

        boolean isPending() {
            return update.toState == PENDING;
        }

        boolean timedOut() {
            return update.toState == PROCESSING && timesOut.before(now());
        }

        boolean isCompleted() {
            return update.toState == COMPLETED;
        }
    }
}
