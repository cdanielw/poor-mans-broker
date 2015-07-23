package org.openforis.rmb.spring;

import org.openforis.rmb.jdbc.JdbcConnectionManager;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class SpringJdbcConnectionManager implements JdbcConnectionManager {
    private final DataSource dataSource;

    public SpringJdbcConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Connection getConnection() throws SQLException {
        return DataSourceUtils.doGetConnection(dataSource);
    }

    public void releaseConnection(Connection connection) {
        try {
            DataSourceUtils.doReleaseConnection(connection, dataSource);
        } catch (SQLException ignore) {
        }
    }
}
