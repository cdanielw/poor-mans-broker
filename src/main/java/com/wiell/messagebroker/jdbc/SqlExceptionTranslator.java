package com.wiell.messagebroker.jdbc;

import com.wiell.messagebroker.spi.MessageRepositoryException;

import java.sql.SQLException;

class SqlExceptionTranslator {
    static final String CONSTRAINT_VIOLATION_CODE = "23";
    private final SQLException exception;
    private final String sqlState;

    SqlExceptionTranslator(SQLException exception) {
        this.exception = exception;
        this.sqlState = exception.getSQLState();
    }

    MessageRepositoryException translate() {
//        if (containsSqlState() && isConstraintViolationException()) {
//            return new EventBrokerConstraintViolationException(exception);
//        }
        return new MessageRepositoryException(exception);
    }

    private boolean containsSqlState() {
        return sqlState != null && sqlState.length() >= 2;
    }

    private boolean isConstraintViolationException() {
        String classCode = sqlState.substring(0, 2);
        return classCode.equals(CONSTRAINT_VIOLATION_CODE);
    }
}
