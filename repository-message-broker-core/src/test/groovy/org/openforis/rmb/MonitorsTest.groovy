package org.openforis.rmb

import org.openforis.rmb.monitor.Event
import org.openforis.rmb.monitor.Monitor
import spock.lang.Specification

class MonitorsTest extends Specification {
    def event = Mock(Event)
    def monitor1 = Mock(Monitor)
    def monitor2 = Mock(Monitor)

    def 'Notifies all monitors, in order'() {
        def monitors = new Monitors([monitor1, monitor2])
        when:
            monitors.onEvent(event)
        then:
            1 * monitor1.onEvent(event)
        then:
            1 * monitor2.onEvent(event)
    }

    def 'Monitors without any monitor can receive an event'() {
        def monitors = new Monitors([])
        when:
            monitors.onEvent(event)
        then:
            notThrown Throwable
    }

    def 'Other monitors are notified even if one fails'() {
        def monitors = new Monitors([monitor1, monitor2])
        when:
            monitors.onEvent(event)
        then:
            1 * monitor1.onEvent(event) >> { throw new RuntimeException('failing monitor') }
        then:
            1 * monitor2.onEvent(event)
    }
}
