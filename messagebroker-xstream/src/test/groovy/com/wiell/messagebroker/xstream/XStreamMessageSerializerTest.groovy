package com.wiell.messagebroker.xstream

import spock.lang.Specification

class XStreamMessageSerializerTest extends Specification {
    def 'When serializing then deserializing a message, the result equals the initial message'() {
        def serializer = new XStreamMessageSerializer()

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
}
