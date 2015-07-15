package com.wiell.messagebroker

import com.wiell.messagebroker.objectserialization.ObjectSerializationMessageSerializer
import spock.lang.Specification
import spock.lang.Unroll

class ObjectSerializationMessageSerializerTest extends Specification {
    def serializer = new ObjectSerializationMessageSerializer()

    @Unroll
    def '"#object" can be serialized and deserialized back to original string'() {

        when:
            def serialized = serializer.serialize(object)
            def result = serializer.deserialize(serialized)

        then:
            result == object

        where:
            object << ['A String', Integer.valueOf(123), new Date()]

    }

    def 'When serializing null, IllegalArgumentException is thrown'() {
        when:
            serializer.serialize(null)

        then:
            thrown(IllegalArgumentException)
    }

    def 'When serializing a non-serializable object, IllegalArgumentException is thrown'() {
        when:
            serializer.serialize(new ByteArrayInputStream(new byte[0]))

        then:
            thrown(IllegalArgumentException)
    }

    def 'When deserializing null, IllegalArgumentException is thrown'() {
        when:
            serializer.deserialize(null)
        then:
            thrown(IllegalArgumentException)
    }


    def 'When deserializing a non-byte array, IllegalArgumentException is thrown'() {
        when:
            serializer.deserialize('not a byte[]')
        then:
            thrown(IllegalArgumentException)
    }

    def 'When deserializing an invalid byte array, DeserilizationFailed is thrown'() {
        when:
            serializer.deserialize(new byte[0])
        then:
            thrown(MessageSerializer.DeserilizationFailed)
    }
}
