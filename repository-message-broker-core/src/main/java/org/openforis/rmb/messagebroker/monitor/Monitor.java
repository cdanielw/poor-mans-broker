package org.openforis.rmb.messagebroker.monitor;

public interface Monitor<T extends Event> {
    void onEvent(T event);
}
