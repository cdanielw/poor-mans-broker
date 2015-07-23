package util

import org.openforis.rmb.monitor.Event
import org.openforis.rmb.monitor.Monitor

import java.util.concurrent.CopyOnWriteArrayList

class CollectingMonitor<T extends Event> implements Monitor<T> {
    final List<T> events = new CopyOnWriteArrayList<>();

    void onEvent(T event) {
        events << event
    }

    List<Class<T>> eventTypes() {
        events.collect { it.class }
    }
}
