package com.wiell.messagebroker.spi;

import com.wiell.messagebroker.MessageConsumer;

public interface MessageCallback {// TODO: Include status - wrap some of this up in a class
    <T> void messageTaken(MessageConsumer<T> consumer, String messageId, String serializedMessage);
}
