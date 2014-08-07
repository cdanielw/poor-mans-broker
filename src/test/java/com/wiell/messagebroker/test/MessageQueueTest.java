package com.wiell.messagebroker.test;

import com.wiell.messagebroker.*;
import com.wiell.messagebroker.jdbc.JdbcBackedMessageBroker;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MessageQueueTest {

    private final MessageBroker messageBroker = new JdbcBackedMessageBroker(null);

    public MessageQueueTest() {
        messageBroker.start();

        asyncTest();
        dispatchTest();
        requestResponseTest();

        messageBroker.stop();
    }

    private void dispatchTest() {
        DispatchingMessageHandler<Event> eventHandler = DispatchingMessageHandler.with(Event.class)
                .add(Created.class, new Created.Handler())
                .add(Deleted.class, new Deleted.Handler())
                .build();

        MessageQueue<Event> queue = messageBroker.queueWith("", Event.class)
                .add(MessageConsumer.with("Event consumer", eventHandler))
                .build();

        queue.publish(new Created());
        queue.publish(new Deleted());
        queue.publish(new Created());
    }

    private void asyncTest() {
        MessageHandler<String> handler1 = new MessageHandler<String>() {
            public void handle(String message) {
                System.out.println("Handler one got " + message);
            }
        };

        MessageHandler<String> handler2 = new MessageHandler<String>() {
            public void handle(String message) {
                System.out.println("Handler two got " + message);
            }
        };

        MessageQueue<String> queue = messageBroker.queueWith("String queue", String.class)
                .add(MessageConsumer.with("Consumer one", handler1).timeout(10, TimeUnit.SECONDS))
                .add(MessageConsumer.with("Consumer two", handler2).timeout(20, TimeUnit.SECONDS))
                .build();

        queue.publish("A message");
    }


    private void requestResponseTest() {
        RespondingMessageHandler<String, Date> handler = new RespondingMessageHandler<String, Date>() {
            public Date handle(String message) {
                try {
                    return new SimpleDateFormat("yyyy-MM-dd").parse(message);
                } catch (ParseException e) {
                    throw new IllegalArgumentException(e);
                }
            }
        };
        MessageConsumer<MessageResponse<String, Date>> consumer1 = MessageConsumer.with(
                "Async consumer one", new MessageHandler<MessageResponse<String, Date>>() {
                    public void handle(MessageResponse<String, Date> messageResponse) {
                        System.out.println("Async consumer one got response "
                                + messageResponse.response + " from message " + messageResponse.message);
                    }
                }).build();

        MessageConsumer<MessageResponse<String, Date>> consumer2 = MessageConsumer.with("Async consumer two", new MessageHandler<MessageResponse<String, Date>>() {
            public void handle(MessageResponse<String, Date> messageResponse) {
                System.out.println("Async consumer two got response "
                        + messageResponse.response + " from message " + messageResponse.message);
            }
        }).build();

        RequestResponseMessageQueue<String, Date> requestResponseQueue =
                messageBroker.queueWith("Request-response queue", handler)
                        .add(consumer1)
                        .add(consumer2)
                        .build();

        Future<Date> dateFuture = requestResponseQueue.publish("2010-05-21");
        try {
            Date date = dateFuture.get();
            System.out.println(date);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
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
        static class Handler implements MessageHandler<Deleted> {
            public void handle(Deleted message) {
                System.out.println("Deleted handler: " + message);
            }
        }
    }
}
