package com.wiell.messagebroker;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DispatchingMessageHandler<M> implements MessageHandler<M> {
    private final Map<Class<? extends M>, MessageHandler<? extends M>> consumerByMessageType;

    private DispatchingMessageHandler(Builder<M> builder) {
        consumerByMessageType = Collections.unmodifiableMap(builder.consumerByMessageType);
    }

    @SuppressWarnings({"unchecked", "SuspiciousMethodCalls"})
    public void handle(M message) {
        MessageHandler<M> consumer = (MessageHandler<M>) consumerByMessageType.get(message.getClass());
        consumer.handle(message);
    }

    @SuppressWarnings("UnusedParameters")
    public static <M> Builder<M> with(Class<M> messageType) {
        return new Builder<M>();
    }

    public static final class Builder<M> {
        private Map<Class<? extends M>, MessageHandler<? extends M>> consumerByMessageType = new HashMap<Class<? extends M>, MessageHandler<? extends M>>();

        public <T extends M> Builder<M> add(Class<T> messageType, MessageHandler<T> consumer) {
            consumerByMessageType.put(messageType, consumer);
            return this;
        }

        public DispatchingMessageHandler<M> build() {
            return new DispatchingMessageHandler<M>(this);
        }
    }
}
