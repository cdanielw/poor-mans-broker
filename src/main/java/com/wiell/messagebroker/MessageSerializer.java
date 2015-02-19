package com.wiell.messagebroker;

public interface MessageSerializer {
    String serialize(Object message);

    Object deserialize(String message);
}
