package org.openforis.rmb.messagebroker.examples.custom;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import org.openforis.rmb.messagebroker.*;
import org.openforis.rmb.messagebroker.examples.Database;
import org.openforis.rmb.messagebroker.jdbc.JdbcMessageRepository;
import org.openforis.rmb.messagebroker.metrics.MetricsMonitor;
import org.openforis.rmb.messagebroker.slf4j.Slf4jLoggingMonitor;
import org.openforis.rmb.messagebroker.spi.ThrottlingStrategy.ExponentialBackoff;
import org.openforis.rmb.messagebroker.xstream.XStreamMessageSerializer;

import java.util.Random;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.*;

public class DirectApiExample {
    public static void main(String[] args) throws InterruptedException {
        Database database = new Database();
        SimpleConnectionManager connectionManager = new SimpleConnectionManager(database.getDataSource());
        JdbcMessageRepository messageRepository = new JdbcMessageRepository(connectionManager, "example_");
        MetricRegistry metricRegistry = new MetricRegistry();

        final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry).build();
        jmxReporter.start();

        final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
                .convertRatesTo(SECONDS)
                .convertDurationsTo(MILLISECONDS)
                .build();
        consoleReporter.start(1, MINUTES);

        RepositoryMessageBroker messageBroker = new RepositoryMessageBroker(
                MessageBrokerConfig.builder(messageRepository, connectionManager)
                        .messageSerializer(new XStreamMessageSerializer())
                        .monitor(new Slf4jLoggingMonitor())
                        .monitor(new MetricsMonitor(metricRegistry))
        ).start();

        final MessageQueue<char[]> queue = messageBroker.<char[]>queueBuilder("A queue")
                .consumer(MessageConsumer.builder("Word Joiner", new CharacterJoiner())
                                .timeout(10, SECONDS)
                                .retry(10, new ExponentialBackoff(1, MINUTES))
                                .messagesHandledInParallel(4)
                )
                .consumer(MessageConsumer.builder("Word Counter", new CharacterCounter())
                                .timeout(5, SECONDS)
                                .neverRetry()
                                .messagesHandledInParallel(1)
                )
                .build();


        Random random = new Random();
        for (int i = 0; i < 1000000; i++) {
            Thread.sleep(random.nextInt(1000));
            publishSomething(queue, connectionManager);
        }

        messageBroker.stop();
        database.stop();
    }

    private static void publishSomething(final MessageQueue<char[]> queue, TransactionManager transactionManager) {
        transactionManager.withTransaction(new Callable<Void>() {
            public Void call() throws Exception {
                Random random = new Random();
                int size = random.nextInt(100);
                char[] chars = new char[size];
                for (int i = 0; i < size; i++) {
                    chars[i] = (char) (random.nextInt(26) + 'a');

                }
                queue.publish(chars);
                return null;
            }
        });
    }

    private static class CharacterCounter implements MessageHandler<char[]> {
        public void handle(char[] chars) {
            System.out.println("Count: " + chars.length);
        }
    }

    private static class CharacterJoiner implements KeepAliveMessageHandler<char[]> {
        Random random = new Random();

        public void handle(char[] chars, KeepAlive keepAlive) {
            String result = "";
            for (Character letter : chars) {
                sleep();
                result += letter + " ";
                keepAlive.send();
            }
            System.out.println("Joint: " + result);
        }

        private void sleep() {
            try {
                Thread.sleep(random.nextInt(500));
            } catch (InterruptedException ignore) {
            }
        }
    }
}
