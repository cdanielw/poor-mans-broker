package com.wiell.messagebroker.xstream

import spock.lang.Specification
import spock.lang.Unroll

import static com.wiell.messagebroker.MessageSerializer.DeserilizationFailed

class XStreamMessageSerializerTest extends Specification {
    def serializer = new XStreamMessageSerializer()

    @Unroll
    def '"#object" can be serialized and deserialized back to original string'() {

        when:
            def serialized = serializer.serialize(message)
            def deserialized = serializer.deserialize(serialized)

        then:
            deserialized == message

        where:
            message << [
                    new Date(),
                    'A string',
                    ['Foo', 'Bar']
            ]
    }

    def 'When serializing null, IllegalArgumentException is thrown'() {
        when:
            serializer.serialize(null)

        then:
            thrown(IllegalArgumentException)
    }

    def 'When deserializing null, IllegalArgumentException is thrown'() {
        when:
            serializer.deserialize(null)
        then:
            thrown(IllegalArgumentException)
    }


    def 'When deserializing a non-string, IllegalArgumentException is thrown'() {
        when:
            serializer.deserialize(new Date())
        then:
            thrown(IllegalArgumentException)
    }

    def 'When deserializing an invalid string, DeserilizationFailed is thrown'() {
        when:
            serializer.deserialize('not valid xstream format')
        then:
            thrown(DeserilizationFailed)
    }
}
