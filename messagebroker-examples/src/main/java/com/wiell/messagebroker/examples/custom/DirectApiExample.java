package com.wiell.messagebroker.examples.custom;

import com.wiell.messagebroker.*;
import com.wiell.messagebroker.examples.Database;
import com.wiell.messagebroker.jdbc.JdbcMessageRepository;
import com.wiell.messagebroker.slf4j.Slf4jLoggingMonitor;
import com.wiell.messagebroker.xstream.XStreamMessageSerializer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static com.wiell.messagebroker.ThrottlingStrategy.ExponentialBackoff;

public class DirectApiExample {
    public static void main(String[] args) throws InterruptedException {
        Database database = new Database();
        SimpleConnectionManager connectionManager = new SimpleConnectionManager(database.getDataSource());
        JdbcMessageRepository messageRepository = new JdbcMessageRepository(connectionManager, "example_");

        PollingMessageBroker messageBroker = new PollingMessageBroker(
                MessageBrokerConfig.builder(messageRepository, connectionManager)
                        .messageSerializer(new XStreamMessageSerializer())
                        .monitor(new Slf4jLoggingMonitor())
        ).start();

        final MessageQueue<List<String>> queue = messageBroker.<List<String>>queueBuilder("A queue")
                .consumer(MessageConsumer.builder("Word Joiner", new WordJoiner())
                                .timeout(10, TimeUnit.SECONDS)
                                .neverRetry()
                                .workerCount(1)
                )
                .consumer(MessageConsumer.builder("Word Counter", new WordCounter())
                                .timeout(5, TimeUnit.SECONDS)
                                .retry(10, new ExponentialBackoff(1, TimeUnit.MINUTES))
                                .workerCount(5)
                )
                .build();

        publishSomething(queue, connectionManager);

        Thread.sleep(1000);
        Thread.sleep(Long.MAX_VALUE);

        messageBroker.stop();
        database.stop();
    }

    private static void publishSomething(final MessageQueue<List<String>> queue, TransactionManager transactionManager) {
        transactionManager.withTransaction(new Callable<Void>() {
            public Void call() throws Exception {
                queue.publish(Arrays.asList("Lorem", "ipsum", "ipsum", "dolor", "sit", "amet"));
                return null;
            }
        });
    }

    private static class WordJoiner implements MessageHandler<List<String>> {
        public void handle(List<String> words) {
            System.out.println("Count: " + words.size());
        }
    }

    private static class WordCounter implements KeepAliveMessageHandler<List<String>> {
        public void handle(List<String> words, KeepAlive keepAlive) {
            for (String word : words) {
                System.out.println(word);
                keepAlive.send();
            }
        }
    }
}
