package com.wiell.messagebroker.examples

import com.wiell.messagebroker.jdbc.JdbcConnectionManager

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.Callable

public final class ThreadLocalConnectionManager implements JdbcConnectionManager {
    private final DataSource dataSource
    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<Connection>()
    private final ThreadLocal<List<Callable<?>>> afterCommitCallbacksHolder = new ThreadLocal<List<Callable<?>>>() {
        protected List<Callable<?>> initialValue() { [] }
    }

    public ThreadLocalConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource
    }

    @SuppressWarnings("GroovyUnusedAssignment")
    public <T> T withTransaction(Callable<T> closure) {
        def connection = null
        boolean newTransaction = !isTransactionRunning()
        try {
            connection = openConnection()
            def result = closure.call()
            if (newTransaction) {
                connection.commit()
                notifyListeners()
            }
            return result
        } catch (Exception e) {
            connection.rollback()
            throw e
        } finally {
            if (newTransaction) {
                connectionHolder.remove()
                afterCommitCallbacksHolder.remove()
                connection?.close()
            }
        }
    }

    Connection getConnection() throws SQLException {
        connectionHolder.get() ?: openConnection()
    }

    void releaseConnection(Connection connection) {
        connectionHolder.remove()
    }

    private boolean isTransactionRunning() {
        connectionHolder.get() != null
    }

    private notifyListeners() {
        afterCommitCallbacksHolder.get().each { it.call() }
    }

    private Connection openConnection() {
        def connection = connectionHolder.get()
        if (connection) return connection
        connection = dataSource.connection
        connection.autoCommit = false
        connectionHolder.set(connection)
        return connection
    }
}
