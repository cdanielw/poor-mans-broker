package org.openforis.rmb.messagebroker.jdbc;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.*;
import org.openforis.rmb.messagebroker.util.Is;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class JdbcMessageRepository implements MessageRepository {
    private final JdbcConnectionManager connectionManager;
    private final String tablePrefix;
    private Clock clock = new Clock.SystemClock();

    public JdbcMessageRepository(JdbcConnectionManager connectionManager, String tablePrefix) {
        this.connectionManager = connectionManager;
        this.tablePrefix = tablePrefix;
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    public void add(final String queueId, final List<MessageConsumer<?>> consumers, final Object serializedMessage) {
        withConnection(new ConnectionCallback() {
            public Void execute(Connection connection) throws SQLException {
                new MessageAdder(connection, tablePrefix, clock)
                        .add(queueId, consumers, serializedMessage);
                return null;
            }
        });
    }

    public void take(final Map<MessageConsumer<?>, Integer> maxCountByConsumer, final MessageTakenCallback callback) {
        withConnection(new ConnectionCallback() {
            public Void execute(Connection connection) throws SQLException {
                new MessageTaker(connection, tablePrefix, clock)
                        .take(maxCountByConsumer, callback);
                return null;
            }
        });
    }

    public boolean update(final MessageProcessingUpdate update) {
        return withConnection(new ConnectionCallback<Boolean>() {
            public Boolean execute(Connection connection) throws SQLException {
                return new MessageProcessingUpdater(connection, tablePrefix, clock)
                        .update(update);
            }
        });
    }

    public void findMessageProcessing(final Collection<MessageConsumer<?>> consumers,
                                      final MessageProcessingFilter filter,
                                      final MessageProcessingFoundCallback callback) {
        Is.notEmpty(consumers, "Must provide at least one consumer");
        Is.notNull(filter, "Filter cannot be null");
        Is.notNull(callback, "Callback cannot be null");
        withConnection(new ConnectionCallback<Void>() {
            public Void execute(Connection connection) throws SQLException {
                new MessageProcessingFinder(connection, tablePrefix, clock)
                        .find(consumers, filter, callback);
                return null;
            }
        });
    }

    public Map<MessageConsumer<?>, Integer> messageCountByConsumer(final Collection<MessageConsumer<?>> consumers, final MessageProcessingFilter filter) {
        return withConnection(new ConnectionCallback<Map<MessageConsumer<?>, Integer>>() {
            public Map<MessageConsumer<?>, Integer> execute(Connection connection) throws SQLException {
                return new MessageCounter(connection, tablePrefix, clock)
                        .countByConsumer(consumers, filter);
            }
        });
    }

    public void deleteMessageProcessing(
            final Collection<MessageConsumer<?>> consumers, final MessageProcessingFilter filter
    ) throws MessageRepositoryException {
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