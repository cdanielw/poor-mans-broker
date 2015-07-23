package org.openforis.rmb.spring

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@ContextConfiguration(locations = "/test-context.xml")
class SpringTest extends Specification {
    @Autowired MessagePublishingService service
    @Autowired MessageCollectingHandler handler

    def 'Can publish messages using Spring injected beans'() {
        when:
            service.publish('A message')

        then:
            new PollingConditions().eventually {
                assert handler.messages == ['A message']
            }
    }
}
