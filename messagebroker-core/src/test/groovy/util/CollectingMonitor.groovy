package util

import com.wiell.messagebroker.monitor.Event
import com.wiell.messagebroker.monitor.Monitor

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
