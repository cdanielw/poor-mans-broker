package com.wiell.messagebroker.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

public interface JdbcConnectionManager {
    Connection getConnection() throws SQLException;

    void releaseConnection(Connection connection) throws SQLException;
}
