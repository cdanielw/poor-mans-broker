package org.openforis.rmb.examples.custom;

import java.sql.Connection;

public interface ConnectionProvider {
    Connection getCurrentConnection();
}
