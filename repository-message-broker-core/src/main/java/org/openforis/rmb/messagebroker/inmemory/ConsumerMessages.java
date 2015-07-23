package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;
import org.openforis.rmb.messagebroker.spi.MessageRepository;

import java.util.*;

class ConsumerMessages {
    MessageConsumer<?> consumer;
    Set<Message> messages = new LinkedHashSet<Message>();
    Map<String, Message> messageById = new HashMap<String, Message>();

    public ConsumerMessages(MessageConsumer<?> consumer) {
        this.consumer = consumer;
    }

    void add(Message message) {
        messages.add(message);
        messageById.put(message.update.messageId, message);
    }

    void apply(MessageProcessingUpdate update) {
        Message message = messageById.get(update.messageId);
        message.setUpdate(update);
        if (message.isCompleted())
            remove(message);
    }

    void remove(Message message) {
        messageById.remove(message.update.messageId);
        messages.remove(message);
    }

    void takePending(Integer maxCount, MessageRepository.MessageTakenCallback callback) {
        List<Message> takenMessages = new ArrayList<Message>();
        int i = 0;
        for (Message message : messages) {
            if (i >= maxCount) break;
            if (message.isPending() || message.timedOut()) {
                message.take();
                takenMessages.add(message);
            }
            i++;
        }
        for (Message message : takenMessages)
            callback.taken(message.update, message.serializedMessage);
    }
}