package org.openforis.rmb.spi;

/**
 * Responsible for serializing/deserializing messages. Implementation should either serialize to String or byte[].
 */
public interface MessageSerializer {
    /**
     * Serialize message to a String or byte[].
     *
     * @param message the message to serialize
     * @return the serialized message
     */
    Object serialize(Object message) throws SerializationFailed;

    /**
     * Deserialize message from a String or byte[].
     *
     * @param serializedMessage the serialized message
     * @return the deserialized message
     */
    Object deserialize(Object serializedMessage) throws DeserilizationFailed;

    final class SerializationFailed extends RuntimeException {
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

    final class DeserilizationFailed extends RuntimeException {
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
