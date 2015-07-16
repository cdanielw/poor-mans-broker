package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.Event;
import com.wiell.messagebroker.monitor.Monitor;

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
