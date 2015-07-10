package com.wiell.messagebroker

import com.wiell.messagebroker.ObjectSerializationMessageSerializer
import spock.lang.Specification
import spock.lang.Unroll

class ObjectSerializationMessageSerializerTest extends Specification {
    @Unroll
    def '"#object" can be serialized and deserialized back to original string'() {
        def serializer = new ObjectSerializationMessageSerializer()

        when:
            String serialized = serializer.serialize(object)
            def result = serializer.deserialize(serialized)
        println()

        then:
            result == object

        where:
            object << ['A String', Integer.valueOf(123), new Date()]

    }
}
