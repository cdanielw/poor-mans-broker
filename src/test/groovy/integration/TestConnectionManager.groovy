package integration

import com.wiell.messagebroker.jdbc.JdbcConnectionManager

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

class TestConnectionManager implements JdbcConnectionManager {
    private final DataSource dataSource
    private Connection connection
    private boolean inTransaction

    TestConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource
    }

    Connection getConnection() {
        if (!connection)
            connection = dataSource.connection
        return connection
    }

    void releaseConnection(Connection connection) throws SQLException {
        if (!inTransaction) {
            connection.close()
            this.connection = null
        }
    }

    void inTransaction(Closure unitOfWork) {
        inTransaction = true
        try {
            connection = dataSource.connection
            unitOfWork()
            connection.commit()
        } finally {
            connection?.close()
            connection = null
            inTransaction = false
        }
    }
}
