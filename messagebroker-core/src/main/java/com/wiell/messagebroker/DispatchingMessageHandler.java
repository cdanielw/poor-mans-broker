package com.wiell.messagebroker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DispatchingMessageHandler<M> implements KeepAliveMessageHandler<M> {
    private final Map<Class<? extends M>, ?> consumerByMessageType;

    private DispatchingMessageHandler(Builder<M> builder) {
        consumerByMessageType = Collections.unmodifiableMap(builder.consumerByMessageType);
    }

    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    public void handle(M message, KeepAlive keepAlive) {
        Object handler = consumerByMessageType.get(message.getClass());
        if (handler instanceof MessageHandler)
            ((MessageHandler<M>) handler).handle(message);
        else
            ((KeepAliveMessageHandler<M>) handler).handle(message, keepAlive);
    }

    @SuppressWarnings("UnusedParameters")
    public static <M> Builder<M> builder(Class<M> messageType) {
        Is.notNull(messageType, "messageType must not be null");
        return new Builder<M>();
    }

    public static final class Builder<M> {
        private Map<Class<? extends M>, Object> consumerByMessageType = new HashMap<Class<? extends M>, Object>();

        public <T extends M> Builder<M> handler(Class<T> messageType, MessageHandler<T> messageHandler) {
            Is.notNull(messageType, "messageType must not be null");
            Is.notNull(messageHandler, "messageHandler must not be null");
            consumerByMessageType.put(messageType, messageHandler);
            return this;
        }

        public <T extends M> Builder<M> handler(Class<T> messageType, KeepAliveMessageHandler<T> messageHandler) {
            Is.notNull(messageType, "messageType must not be null");
            Is.notNull(messageHandler, "messageHandler must not be null");
            consumerByMessageType.put(messageType, messageHandler);
            return this;
        }

        public DispatchingMessageHandler<M> build() {
            return new DispatchingMessageHandler<M>(this);
        }
    }
}
