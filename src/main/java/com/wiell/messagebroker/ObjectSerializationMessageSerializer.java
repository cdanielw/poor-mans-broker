package com.wiell.messagebroker;

import com.wiell.messagebroker.commonscodec.Base64;

import java.io.*;

public final class ObjectSerializationMessageSerializer implements MessageSerializer {
    public String serialize(Object message) throws SerializationFailed {
        ObjectOutputStream objectOutputStream = null;
        try {
            ByteArrayOutputStream bo = new ByteArrayOutputStream();
            objectOutputStream = new ObjectOutputStream(bo);
            objectOutputStream.writeObject(message);
            objectOutputStream.flush();
            return Base64.encodeBase64String(bo.toByteArray());
        } catch (IOException e) {
            throw new SerializationFailed("Failed to serialize " + message, e);
        } finally {
            close(objectOutputStream);
        }
    }

    public Object deserialize(String serializedMessage) throws DeserilizationFailed {
        ObjectInputStream objectInputStream = null;
        try {
            byte[] bytes = Base64.decodeBase64(serializedMessage);
            objectInputStream = new ObjectInputStream(new ByteArrayInputStream(bytes));
            return objectInputStream.readObject();
        } catch (IOException e) {
            throw new SerializationFailed("Failed to deserialize " + serializedMessage, e);
        } catch (ClassNotFoundException e) {
            throw new SerializationFailed("Failed to deserialize " + serializedMessage, e);
        } finally {
            close(objectInputStream);
        }
    }

    private void close(Closeable closeable) {
        try {
            if (closeable != null)
                closeable.close();
        } catch (IOException ignore) {
            ignore.printStackTrace();
        }
    }
}
