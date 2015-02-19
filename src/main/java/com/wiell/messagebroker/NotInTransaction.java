package com.wiell.messagebroker;

public final class NotInTransaction extends RuntimeException {
    public NotInTransaction(String message) {
        super(message);
    }
}
