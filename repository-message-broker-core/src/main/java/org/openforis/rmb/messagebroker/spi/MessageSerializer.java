package org.openforis.rmb.messagebroker.spi;

public interface MessageSerializer {
    Object serialize(Object message) throws SerializationFailed;

    Object deserialize(Object serializedMessage) throws DeserilizationFailed;

    class SerializationFailed extends RuntimeException {
        public SerializationFailed(String errorMessage) {
            super(errorMessage);
        }

        public SerializationFailed(Exception cause) {
            super(cause);
        }

        public SerializationFailed(String errorMessage, Exception cause) {
            super(errorMessage, cause);
        }
    }

    class DeserilizationFailed extends RuntimeException {
        public DeserilizationFailed(String errorMessage) {
            super(errorMessage);
        }

        public DeserilizationFailed(Exception cause) {
            super(cause);
        }

        public DeserilizationFailed(String errorMessage, Exception cause) {
            super(errorMessage, cause);
        }
    }
}
