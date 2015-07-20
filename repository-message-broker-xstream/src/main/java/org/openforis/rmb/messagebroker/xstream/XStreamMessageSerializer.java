package org.openforis.rmb.messagebroker.xstream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.XStreamException;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import org.openforis.rmb.messagebroker.MessageSerializer;

public class XStreamMessageSerializer implements MessageSerializer {
    private final XStream xstream;

    public XStreamMessageSerializer() {
        xstream = new XStream(new StaxDriver());
    }

    public XStreamMessageSerializer(XStream xstream) {
        this.xstream = xstream;
    }

    public Object serialize(Object message) throws SerializationFailed {
        if (message == null)
            throw new IllegalArgumentException("message is null");
        try {
            return xstream.toXML(message);
        } catch (XStreamException e) {
            throw new SerializationFailed("Failed to serialize " + message, e);
        }
    }

    public Object deserialize(Object serializedMessage) throws DeserilizationFailed {
        if (serializedMessage == null)
            throw new IllegalArgumentException("serializedMessage is null");
        if (!(serializedMessage instanceof String))
            throw new IllegalArgumentException("Expected serialized message to be a string. Was a " + serializedMessage.getClass());

        try {
            return xstream.fromXML((String) serializedMessage);
        } catch (XStreamException e) {
            throw new DeserilizationFailed("Failed to deserialize " + serializedMessage, e);
        }
    }
}
