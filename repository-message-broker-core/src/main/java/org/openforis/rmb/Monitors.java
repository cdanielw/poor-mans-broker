package org.openforis.rmb;

import org.openforis.rmb.monitor.Event;
import org.openforis.rmb.monitor.Monitor;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

final class Monitors {
    private final List<Monitor<Event>> monitors;
    private static final Logger LOG = Logger.getLogger(Monitors.class.getName());

    public Monitors(List<Monitor<Event>> monitors) {
        this.monitors = monitors;
    }

    void onEvent(Event event) {
        for (Monitor<Event> monitor : monitors)
            try {
                monitor.onEvent(event);
            } catch (Exception e) {
                // A monitor would normally take care of logging,
                // but if a monitor itself fails, some fallback is needed
                LOG.log(Level.SEVERE, "Monitor failed to handle event. " +
                        "monitor = " + monitor + ", event = " + event);
            }
    }
}
