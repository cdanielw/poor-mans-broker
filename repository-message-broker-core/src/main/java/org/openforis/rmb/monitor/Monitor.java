package org.openforis.rmb.monitor;

/**
 * A message broker monitor. Instances are to be registered when building the message broker.
 * The instance will then get notifications on message broker activities, such as message publication,
 * consumption, and failures.
 *
 * @param <T> the type of events to receive
 */
public interface Monitor<T extends Event> {
    void onEvent(T event);
}
