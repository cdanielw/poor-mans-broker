package org.openforis.rmb;

import org.openforis.rmb.monitor.Event;
import org.openforis.rmb.monitor.Monitor;

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
