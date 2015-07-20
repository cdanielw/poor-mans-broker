package org.openforis.rmb.messagebroker;

import org.openforis.rmb.messagebroker.monitor.Event;
import org.openforis.rmb.messagebroker.monitor.Monitor;

import java.util.List;

final class Monitors {
    private final List<Monitor<Event>> monitors;

    public Monitors(List<Monitor<Event>> monitors) {
        this.monitors = monitors;
    }

    void onEvent(Event event) {
        for (Monitor<Event> monitor : monitors) {
            monitor.onEvent(event);
        }
    }
}
