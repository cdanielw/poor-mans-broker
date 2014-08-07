package com.wiell.messagebroker;


public abstract class AbstractMessageBroker implements MessageBroker {
    private final MessageRepository messageRepository;
    private final MessageSerializer messageSerializer;

    public AbstractMessageBroker(MessageRepository messageRepository, MessageSerializer messageSerializer) {
        this.messageRepository = messageRepository;
        this.messageSerializer = messageSerializer;
    }

    public final void start() {
    }

    public final void stop() {
    }

    public final <M> MessageQueue.Builder<M> queueWith(String queueId, Class<M> messageType) {
        return new MessageQueue.Builder<M>(queueId, messageType, messageRepository, messageSerializer);
    }

    public final <M, R> RequestResponseMessageQueue.Builder<M, R> queueWith(String queueId, RespondingMessageHandler<M, R> responseHandler) {
        return new RequestResponseMessageQueue.Builder<M, R>(queueId, responseHandler, messageRepository, messageSerializer);
    }
}
