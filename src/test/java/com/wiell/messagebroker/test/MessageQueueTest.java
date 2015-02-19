package com.wiell.messagebroker.test;

import com.wiell.messagebroker.*;
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository;

import static com.wiell.messagebroker.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER;
import static java.util.concurrent.TimeUnit.SECONDS;

public class MessageQueueTest {
    private final MessageBroker messageBroker;

    public MessageQueueTest() {
        messageBroker = new PollingMessageBroker(
                MessageBrokerConfig.builder(
                        new InMemoryMessageRepository(),
                        NULL_TRANSACTION_SYNCHRONIZER
                ).abandonedJobsCheckingSchedule(5, SECONDS)
        ).start();

        asyncTest();
        dispatchTest();

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        messageBroker.stop();
    }

    private void dispatchTest() {
        DispatchingMessageHandler<Event> eventHandler = DispatchingMessageHandler.builder(Event.class)
                .handler(Created.class, new Created.Handler())
                .handler(Deleted.class, new Deleted.Handler())
                .build();

        MessageQueue<Event> queue = messageBroker.queueBuilder("The queue", Event.class)
                .consumer(MessageConsumer.builder("Event consumer", eventHandler))
                .build();

        queue.publish(new Created());
        queue.publish(new Deleted());
        queue.publish(new Created());
    }

    private void asyncTest() {
        KeepAliveMessageHandler<String> handler1 = new KeepAliveMessageHandler<String>() {
            public void handle(String message, KeepAlive keepAlive) {
                System.out.println("Handler one got: " + message);
                keepAlive.send();
            }
        };

        MessageHandler<String> handler2 = new MessageHandler<String>() {
            public void handle(String message) {
                System.out.println("Handler two got: " + message);
            }
        };

        MessageQueue<String> queue = messageBroker.queueBuilder("String queue", String.class)
                .consumer(MessageConsumer.builder("Consumer one", handler1).timeout(10, SECONDS))
                .consumer(MessageConsumer.builder("Consumer two", handler2).timeout(20, SECONDS))
                .build();

        queue.publish("A message");
    }

    public static void main(String[] args) {
        new MessageQueueTest();
    }

    private interface Event {
    }

    private static class Created implements Event {
        static class Handler implements MessageHandler<Created> {
            public void handle(Created message) {
                System.out.println("Created handler: " + message);
            }
        }
    }

    private static class Deleted implements Event {
        static class Handler implements KeepAliveMessageHandler<Deleted> {
            public void handle(Deleted message, KeepAlive keepAlive) {
                System.out.println("Deleted handler: " + message);
                keepAlive.send();
            }
        }
    }
}
