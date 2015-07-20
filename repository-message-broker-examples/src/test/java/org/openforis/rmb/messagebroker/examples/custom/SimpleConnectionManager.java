package org.openforis.rmb.messagebroker.examples.custom;

import org.openforis.rmb.messagebroker.TransactionSynchronizer;
import org.openforis.rmb.messagebroker.examples.DatabaseException;
import org.openforis.rmb.messagebroker.jdbc.JdbcConnectionManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

public final class SimpleConnectionManager implements JdbcConnectionManager, TransactionSynchronizer, TransactionManager, ConnectionProvider {
    private final DataSource dataSource;
    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<Connection>();
    private final ThreadLocal<List<CommitListener>> afterCommitCallbacksHolder = new ThreadLocal<List<CommitListener>>() {
        protected List<CommitListener> initialValue() {
            return new ArrayList<CommitListener>();
        }
    };

    public SimpleConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    @SuppressWarnings("GroovyUnusedAssignment")
    public <T> T withTransaction(Callable<T> unitOfWork) {
        Connection connection = null;
        boolean newTransaction = !isInTransaction();
        try {
            connection = openConnection();
            T result = unitOfWork.call();
            if (newTransaction) {
                connection.commit();
                notifyListeners();
            }
            return result;
        } catch (Exception e) {
            rollback(connection);
            throw new DatabaseException(e);
        } finally {
            if (newTransaction) {
                connectionHolder.remove();
                afterCommitCallbacksHolder.remove();
                close(connection);
            }
        }
    }

    public Connection getConnection() throws SQLException {
        Connection connection = connectionHolder.get();
        return connection == null ? openConnection() : connection;
    }

    public Connection getCurrentConnection() {
        Connection connection = connectionHolder.get();
        if (connection == null)
            throw new IllegalStateException("Current connection must be accessed within a transaction");
        return connection;
    }

    public void releaseConnection(Connection connection) {
        connectionHolder.remove();
    }

    public void notifyOnCommit(CommitListener listener) {
        afterCommitCallbacksHolder.get().add(listener);
    }

    public boolean isInTransaction() {
        return connectionHolder.get() != null;
    }

    private void close(Connection connection) {
        if (connection != null)
            try {
                connection.close();
            } catch (SQLException ignore) {
            }
    }

    private void rollback(Connection connection) {
        if (connection != null)
            try {
                connection.rollback();
            } catch (SQLException e) {
                throw new DatabaseException("Failed to rollback connection", e);
            }
    }

    private void notifyListeners() {
        for (CommitListener listener : afterCommitCallbacksHolder.get()) {
            try {
                listener.committed();
            } catch (Exception e) {
                throw new CommitCallbackException(e);
            }
        }
    }

    private Connection openConnection() {
        try {
            Connection connection = connectionHolder.get();
            if (connection != null) return connection;
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            connectionHolder.set(connection);
            return connection;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    public static class CommitCallbackException extends RuntimeException {
        public CommitCallbackException(Throwable cause) {
            super(cause);
        }
    }
}