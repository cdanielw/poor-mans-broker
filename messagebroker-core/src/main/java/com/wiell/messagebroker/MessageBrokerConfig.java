package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.Event;
import com.wiell.messagebroker.monitor.Monitor;
import com.wiell.messagebroker.objectserialization.ObjectSerializationMessageSerializer;
import com.wiell.messagebroker.util.Is;

import java.util.ArrayList;
import java.util.List;

public final class MessageBrokerConfig {
    final MessageRepository messageRepository;
    final TransactionSynchronizer transactionSynchronizer;
    final MessageSerializer messageSerializer;
    final Monitors monitors;

    private MessageBrokerConfig(Builder builder) {
        this.messageRepository = builder.messageRepository;
        this.transactionSynchronizer = builder.transactionSynchronizer;
        this.messageSerializer = builder.messageSerializer;
        this.monitors = new Monitors(builder.monitors);
    }

    public String toString() {
        return "MessageBrokerConfig{" +
                "messageRepository=" + messageRepository +
                ", transactionSynchronizer=" + transactionSynchronizer +
                ", messageSerializer=" + messageSerializer +
                ", monitors=" + monitors +
                '}';
    }

    public static Builder builder(MessageRepository messageRepository,
                                  TransactionSynchronizer transactionSynchronizer) {
        Is.notNull(messageRepository, "messageRepository must not be null");
        Is.notNull(transactionSynchronizer, "transactionSynchronizer must not be null");
        return new Builder(messageRepository, transactionSynchronizer);
    }

    public static final class Builder {
        private final MessageRepository messageRepository;
        private final TransactionSynchronizer transactionSynchronizer;
        private MessageSerializer messageSerializer = new ObjectSerializationMessageSerializer();
        private final List<Monitor<Event>> monitors = new ArrayList<Monitor<Event>>();

        private Builder(MessageRepository messageRepository, TransactionSynchronizer transactionSynchronizer) {
            this.messageRepository = messageRepository;
            this.transactionSynchronizer = transactionSynchronizer;
        }

        public Builder messageSerializer(MessageSerializer messageSerializer) {
            this.messageSerializer = messageSerializer;
            return this;
        }

        public Builder monitor(Monitor<Event> monitor) {
            monitors.add(monitor);
            return this;
        }

        public MessageBrokerConfig build() {
            return new MessageBrokerConfig(this);
        }
    }
}
