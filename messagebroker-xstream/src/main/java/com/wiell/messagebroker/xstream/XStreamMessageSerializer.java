package com.wiell.messagebroker.xstream;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.StaxDriver;
import com.wiell.messagebroker.MessageSerializer;

public class XStreamMessageSerializer implements MessageSerializer {
    private final XStream xstream = new XStream(new StaxDriver());

    public String serialize(Object message) throws SerializationFailed {
        return xstream.toXML(message);
    }

    public Object deserialize(String serializedMessage) throws DeserilizationFailed {
        return xstream.fromXML(serializedMessage);
    }
}
