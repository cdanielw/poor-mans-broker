package integration

import org.openforis.rmb.messagebroker.jdbc.JdbcConnectionManager

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

class TestConnectionManager implements JdbcConnectionManager {
    private final DataSource dataSource
    private final ThreadLocal<Connection> connectionHolder = new ThreadLocal<Connection>()
    private final ThreadLocal<List<Closure>> afterCommitCallbacksHolder = new ThreadLocal<List<Closure>>() {
        protected List<Closure> initialValue() { [] }
    }

    TestConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource
    }

    @SuppressWarnings("GroovyUnusedAssignment")
    public <T> T withTransaction(Closure<T> closure) {
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
