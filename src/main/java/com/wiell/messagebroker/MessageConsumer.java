package com.wiell.messagebroker;

import java.util.concurrent.TimeUnit;

public final class MessageConsumer<M> {
    public final String id;
    private final MessageHandler<M> handler;
    public final int time;
    public final TimeUnit timeUnit;

    private MessageConsumer(Builder<M> builder) {
        id = builder.consumerId;
        handler = builder.handler;
        time = builder.time;
        timeUnit = builder.timeUnit;
    }

    public void consume(M message) {
        handler.handle(message);
    }

    public static <M> Builder<M> with(String consumerId, MessageHandler<M> handler) {
        return new Builder<M>(consumerId, handler);
    }

    public static final class Builder<M> {
        private final String consumerId;
        private final MessageHandler<M> handler;
        private int time;
        private TimeUnit timeUnit;

        public Builder(String consumerId, MessageHandler<M> handler) {
            this.consumerId = consumerId;
            this.handler = handler;
        }

        public Builder<M> timeout(int time, TimeUnit timeUnit) {
            this.time = time;
            this.timeUnit = timeUnit;
            return this;
        }

        public MessageConsumer<M> build() {
            return new MessageConsumer<M>(this);
        }
    }
}
