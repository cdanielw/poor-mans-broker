package com.wiell.messagebroker.examples.custom;

import java.sql.Connection;

public interface ConnectionProvider {
    Connection getCurrentConnection();
}
