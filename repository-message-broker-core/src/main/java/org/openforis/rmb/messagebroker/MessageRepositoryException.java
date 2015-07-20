package org.openforis.rmb.messagebroker;

public class MessageRepositoryException extends RuntimeException {
    public MessageRepositoryException(Exception cause) {
        super(cause);
    }

    public MessageRepositoryException(String message, Exception cause) {
        super(message, cause);
    }
}
