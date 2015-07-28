package org.openforis.rmb.spring

import org.openforis.rmb.monitor.Event
import org.openforis.rmb.monitor.Monitor

import java.util.concurrent.CopyOnWriteArrayList

class EventCollectingMonitor implements Monitor<Event> {
    final List<Event> events = new CopyOnWriteArrayList()

    void onEvent(Event event) {
        events << event
    }
}
