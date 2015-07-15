package util

import com.wiell.messagebroker.monitor.Event
import com.wiell.messagebroker.monitor.Monitor

import java.util.concurrent.CopyOnWriteArrayList

class CollectingMonitor implements Monitor {
    final List<Event> events = new CopyOnWriteArrayList<>();

    void onEvent(Event event) {
        events << event
    }

    List<Class<? extends Event>> eventTypes() {
        events.collect { it.class }
    }
}
