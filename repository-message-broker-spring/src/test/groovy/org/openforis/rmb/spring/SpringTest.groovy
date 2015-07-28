package org.openforis.rmb.spring

import org.openforis.rmb.monitor.*
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.test.context.ContextConfiguration
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

@ContextConfiguration(locations = "/test-context.xml")
class SpringTest extends Specification {
    @Autowired MessagePublishingService service
    @Autowired MessageCollectingHandler handler
    @Autowired EventCollectingMonitor monitor

    def 'Can publish messages using Spring injected beans, and have monitors notified'() {
        when:
            service.publish('A message')

        then:
            new PollingConditions().eventually {
                def eventTypes = monitor.events.collect { it.class }
                assert eventTypes.containsAll([
                        MessageQueueCreatedEvent,
                        MessagePublishedEvent,
                        PollingForMessagesEvent,
                        ConsumingNewMessageEvent,
                        MessageConsumedEvent
                ])
                assert handler.messages == ['A message']
            }
    }
}
