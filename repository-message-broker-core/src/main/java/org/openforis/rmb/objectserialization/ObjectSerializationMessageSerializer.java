package org.openforis.rmb.objectserialization;

import org.openforis.rmb.spi.MessageSerializer;
import org.openforis.rmb.util.Is;

import java.io.*;

public final class ObjectSerializationMessageSerializer implements MessageSerializer {
    public Object serialize(Object message) throws SerializationFailed {
        Is.notNull(message, "message must not be null");
        if (!(message instanceof Serializable))
            throw new IllegalArgumentException("message must be Serializable. Message type: " + message.getClass());

        ObjectOutputStream objectOutputStream = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(bo);
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            return bo.toByteArray();
        } catch (IOException e) {
            throw new SerializationFailed("Failed to serialize " + message, e);
        } finally {
            close(objectOutputStream);
        }
    }

    public Object deserialize(Object serializedMessage) throws DeserilizationFailed {
        if (serializedMessage == null)
            throw new IllegalArgumentException("serializedMessage is null");
        if (!(serializedMessage instanceof byte[]))
            throw new IllegalArgumentException("Expected serializedMessage to be a byte[]. Was a " + serializedMessage.getClass());
        byte[] bytes = (byte[]) serializedMessage;

        ObjectInputStream objectInputStream = null;
        try {
            objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return objectInputStream.readObject();
        } catch (Exception e) {
            throw new DeserilizationFailed(e);
        } finally {
            close(objectInputStream);
        }
    }

    private void close(Closeable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (IOException ignore) {
        }
    }
}
