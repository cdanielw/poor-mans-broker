package com.wiell.messagebroker;

public final class NotInTransaction extends RuntimeException {
    public NotInTransaction() {
        super("Trying to publish message outside of a transaction");
    }
}
