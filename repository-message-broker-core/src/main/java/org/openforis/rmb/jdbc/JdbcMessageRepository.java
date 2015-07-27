package org.openforis.rmb.jdbc;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.*;
import org.openforis.rmb.util.Is;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

// @formatter:off
/**
 * A {@link MessageRepository} backed by a JDBC.
 * <p>
 * It guarantees at-least-once delivery of messages, in order. The number of messages to be handled in parallel by a
 * consumer is also guaranteed, event for clustered deployments.
 * </p>
 * <p>
 * JDBC connections are provided through an implementation of {@link JdbcConnectionManager}. This is required
 * to ensure that messages can be published within a larger transaction. The Spring module provides an implementation
 * returning the JDBC connections from the current transaction.
 * </p>
 * <p>
 * A table prefix needs to be specified. The prefix will be prepended to all table names.
 * </p>
 * <strong>Schema for PostgreSQL, with 'example_' as table prefix:</strong>
 * <pre>
 * {@code

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
 * }
 * </pre>
 *
 */
// @formatter:on
public final class JdbcMessageRepository implements MessageRepository {
    private final JdbcConnectionManager connectionManager;
    private final String tablePrefix;
    private Clock clock = new Clock.SystemClock();

    public JdbcMessageRepository(JdbcConnectionManager connectionManager, String tablePrefix) {
        this.connectionManager = connectionManager;
        this.tablePrefix = tablePrefix;
    }

    void setClock(Clock clock) {
        Is.notNull(clock, "clock must not be null");
        this.clock = clock;
    }

    public void add(
            final String queueId,
            final List<MessageConsumer<?>> consumers,
            final Object serializedMessage
    ) {
        Is.hasText(queueId, "queueId must be specified");
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(serializedMessage, "serializedMessage must not be null");
        withConnection(new ConnectionCallback() {
            public Void execute(Connection connection) throws SQLException {
                new MessageAdder(connection, tablePrefix, clock)
                        .add(queueId, consumers, serializedMessage);
                return null;
            }
        });
    }

    public void take(
            final Map<MessageConsumer<?>, Integer> maxCountByConsumer,
            final MessageTakenCallback callback
    ) {
        Is.notEmpty(maxCountByConsumer, "maxCountByConsumer must not be empty");
        Is.notNull(callback, "callback must not be null");
        withConnection(new ConnectionCallback() {
            public Void execute(Connection connection) throws SQLException {
                new MessageTaker(connection, tablePrefix, clock)
                        .take(maxCountByConsumer, callback);
                return null;
            }
        });
    }

    public boolean update(
            final MessageProcessingUpdate update
    ) {
        Is.notNull(update, "update must not be null");
        return withConnection(new ConnectionCallback<Boolean>() {
            public Boolean execute(Connection connection) throws SQLException {
                return new MessageProcessingUpdater(connection, tablePrefix, clock)
                        .update(update);
            }
        });
    }

    public void findMessageProcessing(
            final Collection<MessageConsumer<?>> consumers,
            final MessageProcessingFilter filter,
            final MessageProcessingFoundCallback callback
    ) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(filter, "filter must not be null");
        Is.notNull(callback, "callback must not be null");
        withConnection(new ConnectionCallback<Void>() {
            public Void execute(Connection connection) throws SQLException {
                new MessageProcessingFinder(connection, tablePrefix, clock)
                        .find(consumers, filter, callback);
                return null;
            }
        });
    }

    public Map<MessageConsumer<?>, Integer> messageCountByConsumer(
            final Collection<MessageConsumer<?>> consumers,
            final MessageProcessingFilter filter
    ) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(filter, "filter must not be null");
        return withConnection(new ConnectionCallback<Map<MessageConsumer<?>, Integer>>() {
            public Map<MessageConsumer<?>, Integer> execute(Connection connection) throws SQLException {
                return new MessageCounter(connection, tablePrefix, clock)
                        .countByConsumer(consumers, filter);
            }
        });
    }

    public void deleteMessageProcessing(
            final Collection<MessageConsumer<?>> consumers,
            final MessageProcessingFilter filter
    ) {
        Is.notEmpty(consumers, "consumers must not be empty");
        Is.notNull(filter, "filter must not be null");
        withConnection(new ConnectionCallback<Void>() {
            public Void execute(Connection connection) throws SQLException {
                new MessageDeleter(connection, tablePrefix, clock)
                        .delete(consumers, filter);
                return null;
            }
        });
    }

    private <T> T withConnection(ConnectionCallback<T> callback) {
        Connection connection = null;
        try {
            connection = connectionManager.getConnection();
            connection.setAutoCommit(false);
            return callback.execute(connection);
        } catch (SQLException e) {
            throw new MessageRepositoryException(e);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    private interface ConnectionCallback<T> {
        T execute(Connection connection) throws SQLException;
    }
}