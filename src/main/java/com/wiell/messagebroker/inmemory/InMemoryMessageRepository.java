package com.wiell.messagebroker.inmemory;

import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.MessageProcessingJob;
import com.wiell.messagebroker.MessageProcessingJobRequest;
import com.wiell.messagebroker.MessageRepository;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class InMemoryMessageRepository implements MessageRepository {
    private final ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>> messagesByConsumer = new ConcurrentHashMap<String, ConcurrentLinkedQueue<Message>>();

    @SuppressWarnings("unchecked")
    public void enqueue(String queueId, List<MessageConsumer<?>> consumers, Object message) {
        for (MessageConsumer<?> consumer : consumers) {
            ConcurrentLinkedQueue<Message> newConsumerMessages = new ConcurrentLinkedQueue<Message>();
            ConcurrentLinkedQueue<Message> oldConsumerMessages = messagesByConsumer.putIfAbsent(consumer.id, newConsumerMessages);
            ConcurrentLinkedQueue<Message> consumerMessages = oldConsumerMessages == null ? newConsumerMessages : oldConsumerMessages;
            consumerMessages.add(new Message(UUID.randomUUID().toString(), message));
        }
    }

    public void takeJobs(Collection<MessageProcessingJobRequest> requests, MessageProcessingJob.Callback callback) {
        for (MessageProcessingJobRequest request : requests)
            takeConsumerJobs(request, callback);
    }

    private void takeConsumerJobs(MessageProcessingJobRequest request, MessageProcessingJob.Callback callback) {
        ConcurrentLinkedQueue<Message> messages = messagesByConsumer.get(request.consumerId);
        if (messages == null)
            return;
        for (int i = 0; i < request.messageCount; i++) {
            Message message = messages.poll();
            if (message == null)
                return;
            callback.onJob(new MessageProcessingJob(message.id, message.body, request.consumerId));
        }
    }

    public void keepAlive(MessageProcessingJob job) {
        // No need for keep-alive.
        // Since this repository doesn't share data between processes, it's pointless to check for abandoned jobs.

        // TODO: Interrupt jobs that timed out?
    }

    public void completed(MessageProcessingJob job) {
        // TODO: Implement
    }

    public void failed(MessageProcessingJob job, int retries, Exception exception) {
        // TODO: Implement
    }

    private static class Message {
        final String id;
        final Object body;

        Message(String id, Object body) {
            this.id = id;
            this.body = body;
        }
    }
}
