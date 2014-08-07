package com.wiell.messagebroker;


public abstract class AbstractMessageBroker implements MessageBroker {
    private final MessageQueueManager queueManager;

    public AbstractMessageBroker(MessageRepository messageRepository, MessageSerializer messageSerializer) {
        queueManager = new MessageQueueManager(messageRepository, messageSerializer);
    }

    public final void start() {
        queueManager.start();
    }

    public final void stop() {
        queueManager.stop();
    }

    public final <M> MessageQueue.Builder<M> queueWith(String queueId, Class<M> messageType) {
        return new MessageQueue.Builder<M>(queueId, messageType, queueManager);
    }

    public final <M, R> RequestResponseMessageQueue.Builder<M, R> queueWith(String queueId, RespondingMessageHandler<M, R> responseHandler) {
        return new RequestResponseMessageQueue.Builder<M, R>(queueId, responseHandler, queueManager);
    }
}
