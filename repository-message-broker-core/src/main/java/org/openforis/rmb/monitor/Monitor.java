package org.openforis.rmb.monitor;

public interface Monitor<T extends Event> {
    void onEvent(T event);
}
