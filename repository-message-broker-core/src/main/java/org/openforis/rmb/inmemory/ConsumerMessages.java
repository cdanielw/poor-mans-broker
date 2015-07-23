package org.openforis.rmb.inmemory;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.MessageProcessingUpdate;
import org.openforis.rmb.spi.MessageRepository;

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
        messageById.put(message.update.getMessageId(), message);
    }

    void apply(MessageProcessingUpdate update) {
        Message message = messageById.get(update.getMessageId());
        message.setUpdate(update);
        if (message.isCompleted())
            remove(message);
    }

    void remove(Message message) {
        messageById.remove(message.update.getMessageId());
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