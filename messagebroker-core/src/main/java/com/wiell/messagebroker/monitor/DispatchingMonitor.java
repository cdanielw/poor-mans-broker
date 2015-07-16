package com.wiell.messagebroker.monitor;

import com.wiell.messagebroker.util.Is;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class DispatchingMonitor implements Monitor<Event> {
    private final Map<Class<? extends Event>, Monitor<?>> monitorByEventType;

    private DispatchingMonitor(Builder builder) {
        monitorByEventType = Collections.unmodifiableMap(builder.monitorByEventType);
    }

    @SuppressWarnings("unchecked")
    public void onEvent(Event event) {
        Monitor monitor = monitorByEventType.get(event.getClass());
        if (monitor != null)
            monitor.onEvent(event);
    }

    @SuppressWarnings("UnusedParameters")
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Map<Class<? extends Event>, Monitor<?>> monitorByEventType =
                new HashMap<Class<? extends Event>, Monitor<?>>();

        private Builder() { }

        public <T extends Event> Builder monitor(Class<T> eventType, Monitor<T> monitor) {
            Is.notNull(eventType, "eventType must not be null");
            Is.notNull(monitor, "monitor must not be null");
            monitorByEventType.put(eventType, monitor);
            return this;
        }

        public DispatchingMonitor build() {
            return new DispatchingMonitor(this);
        }
    }
}
