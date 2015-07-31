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
to get notified of what is happening. Two monitors are provided, in separate modules.

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

Usage example
-------------
```java
    MessageBroker messageBroker = RepositoryMessageBroker.builder(              // (1)
            new JdbcMessageRepository(connectionManager, "example_"),           // (2)
            transactionSynchronizer)                                            // (3)
            .monitor(new Slf4jLoggingMonitor())                                 // (4)
            .monitor(new MetricsMonitor(new MetricRegistry()))                  // (5)
            .build();

    MessageQueue<Date> queue = messageBroker.<Date>queueBuilder("A test queue") // (6)
            .consumer(MessageConsumer.builder("A consumer",
                    (date) -> System.out.println("Got a date: " + date)))       // (7)
            .build();

    messageBroker.start();

    withTransaction(() -> {                                                     // (8)
        doSomeTransactionalWork();
        queue.publish(new Date(0));                                             // (9)
        queue.publish(new Date(100));
    });

    messageBroker.stop();
```

1. The message broker is responsible for creating message queues, and keeps an eye on it's message queues,
picking up abandoned messages, monitor queue sizes etc.
2. The MessageRepository is working with the underlying repository.
The `JdbcMessageRepository` need a `ConnectionManager` implementation, to get and resource JDBC connections.
An implementation using Spring's `DataSourceUtils` is provided. A table prefix can also be specified here.
3. A `TransactionSynchronizer` implementation also needs to be specified. It is responsible for checking
if a transaction is active, and allows listeners to be notified when current transaction commits.
Just as with the connection manager, an implementation using Spring's `TransactionSynchronizationManager` is provided.
4. Registers a monitor that logs the message broker activities using SLF4j.
5. Registers a monitor capturing metrics about the message broker, using Dropwizard's Metrics.
6. A `MessageQueue` is the object where messages are published. `MessageConsumer`s are registered at the time
the queue is built.
7. A `MessageConsumer` specifies a `MessageHandler`, which will receive published messages. In addition to
`MessageHandler`s, there are `KeepAliveMessageHandler`s, which provides a way for the handler to
notify the message broker it's still alive, and prevents it from timing out.
8. Messages must be published within a transaction.
9. Example of how messages are published. The message is written to the database in the same transaction
as the transactional work is being done. Once the transaction commits, the message broker will query
the database for messages to process, and forwards the message to the message handler.


JDBC Schema
-----------
If the JdbcMessageRepository is used, the following two tables are needed. In this example,
the database is PostgreSQL, and the tablePrefix is set to "example_"..

```sql
    CREATE TABLE example_message (
    id               VARCHAR(127) NOT NULL,
    sequence_no      SERIAL,
    publication_time TIMESTAMP    NOT NULL,
    queue_id         VARCHAR(127) NOT NULL,
    message_string   TEXT,
    message_bytes    BYTEA,
    PRIMARY KEY (id)
    );

    CREATE TABLE example_message_processing (
    message_id    VARCHAR(127) NOT NULL,
    consumer_id   VARCHAR(127) NOT NULL,
    version_id    VARCHAR(127) NOT NULL,
    state         VARCHAR(32)  NOT NULL,
    last_updated  TIMESTAMP    NOT NULL,
    times_out     TIMESTAMP    NOT NULL,
    retries       INTEGER      NOT NULL,
    error_message TEXT,
    PRIMARY KEY (message_id, consumer_id),
    FOREIGN KEY (message_id) REFERENCES example_message (id)
    );
```

Spring XML examples
-------------------
*Minimal:*

```xml
    <bean id="messageBroker" class="org.openforis.rmb.spring.SpringJdbcMessageBroker">
        <constructor-arg value="#{database.dataSource}"/>
        <property name="tablePrefix" value="example_"/>
    </bean>

    <bean id="messageQueue" class="org.openforis.rmb.spring.SpringMessageQueue">
        <constructor-arg ref="messageBroker"/>
        <constructor-arg value="My queue id"/>
        <constructor-arg>
            <list>
                <bean class="org.openforis.rmb.spring.SpringMessageConsumer">
                    <constructor-arg value="My consumer id"/>
                    <constructor-arg ref="messageHandler"/>
                </bean>
            </list>
        </constructor-arg>
    </bean>
```

*Fully configured:*
```xml
    <bean id="messageBroker" class="org.openforis.rmb.spring.SpringJdbcMessageBroker">
        <constructor-arg value="#{database.dataSource}"/>
        <property name="tablePrefix" value="example_"/>
        <property name="messageSerializer">
            <bean class="org.openforis.rmb.xstream.XStreamMessageSerializer"/>
        </property>
        <property name="monitors">
            <list>
                <bean class="org.openforis.rmb.slf4j.Slf4jLoggingMonitor"/
                <ref bean="myCustomMonitor"/>
            </list>
        </property>
        <property name="repositoryWatcherPollingPeriodSeconds" value="10"/>
    </bean>

    <bean id="messageQueue" class="org.openforis.rmb.spring.SpringMessageQueue">
        <constructor-arg ref="messageBroker"/>
        <constructor-arg value="My queue id"/>
        <constructor-arg>
            <list>
                <bean class="org.openforis.rmb.spring.SpringMessageConsumer">
                    <constructor-arg value="My consumer id"/>
                    <constructor-arg ref="messageHandler"/>
                    <property name="messagesHandledInParallel" value="1"/>
                    <property name="retries" value="5"/>
                    <property name="throttlingStrategy">
                        <bean class="org.openforis.rmb.spi.ThrottlingStrategy$ExponentialBackoff">
                            <constructor-arg value="1"/>
                            <constructor-arg value="MINUTES"/>
                        </bean>
                    </property>
                    <property name="timeoutSeconds" value="30"/>
                </bean>
            </list>
        </constructor-arg>
    </bean>
```

Maven dependencies
------------------
```xml
    <dependency>
        <groupId>org.openforis.rmb</groupId>
        <artifactId>repository-message-broker-core</artifactId>
        <version>${repository-message-broker.version}</version>
    </dependency>

    <dependency>
        <groupId>org.openforis.rmb</groupId>
        <artifactId>repository-message-broker-metrics</artifactId>
        <version>${repository-message-broker.version}</version>
    </dependency>

    <dependency>
        <groupId>org.openforis.rmb</groupId>
        <artifactId>repository-message-broker-slf4j</artifactId>
        <version>${repository-message-broker.version}</version>
    </dependency>

    <dependency>
        <groupId>org.openforis.rmb</groupId>
        <artifactId>repository-message-broker-spring</artifactId>
        <version>${repository-message-broker.version}</version>
    </dependency>

    <dependency>
        <groupId>org.openforis.rmb</groupId>
        <artifactId>repository-message-broker-xstream</artifactId>
        <version>${repository-message-broker.version}</version>
    </dependency>
```