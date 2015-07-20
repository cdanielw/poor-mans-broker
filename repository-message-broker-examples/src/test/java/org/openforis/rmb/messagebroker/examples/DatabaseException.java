package org.openforis.rmb.messagebroker.examples;

public class DatabaseException extends RuntimeException {
    public DatabaseException(Throwable cause) {
        super(cause);
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }
}
