package com.wiell.messagebroker;

final class MessageQueueManager {
    private final MessageRepository messageRepository;
    private final MessageSerializer messageSerializer;

    MessageQueueManager(MessageRepository messageRepository, MessageSerializer messageSerializer) {
        this.messageRepository = messageRepository;
        this.messageSerializer = messageSerializer;
    }

    <M> void publish(String queueId, M message, Iterable<MessageConsumer<M>> messageConsumers) {
        messageRepository.submit(queueId, messageSerializer.serialize(message), messageConsumers);
//            doAsync() { // TODO: If queue is already polling for messages, skip
//
//            }
        // No need to poll for messages if the global poll is running
    }

    void start() {
        // TODO: Setup global poll
    }

    void stop() {

    }

    void registerQueue(MessageQueue queue) {
        // Keep reference of queue, and perhaps create a thread for it?
    }

    void registerQueue(RequestResponseMessageQueue queue) {

    }
}
