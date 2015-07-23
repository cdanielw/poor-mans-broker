package org.openforis.rmb.spring

import org.openforis.rmb.monitor.Event
import org.openforis.rmb.monitor.Monitor

class EventCollectingMonitor implements Monitor<Event> {
    final List<Event> events = []

    void onEvent(Event event) {
        events << event
    }
}
