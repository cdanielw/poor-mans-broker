package com.wiell.messagebroker.jdbc;

import com.wiell.messagebroker.AbstractMessageBroker;

import javax.sql.DataSource;

public class JdbcBackedMessageBroker extends AbstractMessageBroker {
    public JdbcBackedMessageBroker(DataSource dataSource) {
        super(null);
    }
}
