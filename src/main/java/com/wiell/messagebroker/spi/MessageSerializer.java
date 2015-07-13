package com.wiell.messagebroker.spi;

public interface MessageSerializer {
    String serialize(Object message) throws SerializationFailed;

    Object deserialize(String serializedMessage) throws DeserilizationFailed;


    class SerializationFailed extends RuntimeException {
        public SerializationFailed(String message) {
            super(message);
        }

        public SerializationFailed(String message, Throwable cause) {
            super(message, cause);
        }
    }

    class DeserilizationFailed extends RuntimeException {
        public DeserilizationFailed(String message) {
            super(message);
        }

        public DeserilizationFailed(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
