package org.openforis.rmb.messagebroker.examples.custom;

import java.sql.Connection;

public interface ConnectionProvider {
    Connection getCurrentConnection();
}
