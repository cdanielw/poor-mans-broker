Repository Message Broker
=========================
When doing some transactional work with a repository, such as a relational database,
often some external resource must be updated or notified at the same time.
Examples of this would be to update a Lucene index, send an email, and make an HTTP POST, when storing a record in a database.

A typical solution of this is to use a distributed transaction to submit the work to be done into a message queue.
The queue would then guarantee that the work is completed.
This requires a lot of infrastructure, to understand, test, and maintain.

Repository Message Broker takes a similar approach, but instead of using distributed transactions
and an external message queue, it keeps the message queue in the repository where the transactional work is done to
start with. That way, messages can be put in the queue using local transactions.
It provides the same benefits with no extra infrastructure.

Lightweight and extensible
--------------------------
The core module has no dependencies outside of JDK 6, not even a logging framework.
Dependencies to third-party libraries have been separated into their own modules.

The library have been designed to be extensible, and allows monitors to be registered,
to get notified of what is happening. Two monitors are provided, in separate monitors.

* `repository-message-broker-slf4j` provides logging through `org.openforis.rmb.slf4j.Slf4jLoggingMonitor`
* `repository-message-broker-metrics` provides metrics to a Dropwizard's Metrics MetricRegistry through
  `org.openforis.rmb.metrics.MetricsMonitor`

Support for storing messages using a JDBC DataSource is included in the core module,
as well as an in-memory repository for testing.

By default, Java object serialization is used when storing messages in the repository.
An XStream message serializer, `org.openforis.rmb.xstream.XStreamMessageSerializer`,
is provided by `repository-message-broker-xstream`,
which is preferable to object serialization in many cases.

To make it easy for Springframework users, `repository-message-broker-spring` provides integration with
Spring's transaction manager, and provides helper classes to make it easy to configure queues using Spring.

Examples
--------
```java
    RepositoryMessageBroker messageBroker = new RepositoryMessageBroker(        // (1)
            MessageBrokerConfig.builder(
                    new JdbcMessageRepository(connectionManager, "example_"),   // (2)
                    transactionSynchronizer                                     // (3)
            )
    );

    MessageQueue<Date> queue = messageBroker.<Date>queueBuilder("A test queue") // (4)
            .consumer(MessageConsumer.builder("A consumer",
                    (date) -> System.out.println("Got a date: " + date)))       // (5)
            .build();

    messageBroker.start();
    queue.publish(new Date(0));                                                  // (6)
    queue.publish(new Date(100));
    messageBroker.stop();
```

1. The message broker is responsible for creating message queues.
2.