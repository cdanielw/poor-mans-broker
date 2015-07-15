package com.wiell.messagebroker.objectserialization;

import com.wiell.messagebroker.MessageSerializer;

import java.io.*;

public final class ObjectSerializationMessageSerializer implements MessageSerializer {
    public Object serialize(Object message) throws SerializationFailed {
        if (message == null)
            throw new IllegalArgumentException("message is null");
        if (!(message instanceof Serializable))
            throw new IllegalArgumentException("Expected serialized message to be Serializable. Message type: " + message.getClass());

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
        } catch (IOException e) {
            throw new DeserilizationFailed(e);
        } catch (ClassNotFoundException e) {
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
