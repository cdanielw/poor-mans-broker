package com.wiell.messagebroker;

import com.wiell.messagebroker.monitor.Event;
import com.wiell.messagebroker.monitor.Monitor;

import java.util.List;

final class Monitors {
    private final List<Monitor> monitors;

    public Monitors(List<Monitor> monitors) {
        this.monitors = monitors;
    }

    void onEvent(Event event) {
        for (Monitor monitor : monitors) {
            monitor.onEvent(event);
        }
    }
}
