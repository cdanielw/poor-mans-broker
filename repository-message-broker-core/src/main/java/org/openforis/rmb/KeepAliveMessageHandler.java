package org.openforis.rmb;

/**
 * A message handler that allows the message broker to be notified that progress is still being made on a message,
 * through a {@link KeepAlive}.
 *
 * @param <M> the type of messages to be handled
 */
public interface KeepAliveMessageHandler<M> {
    /**
     * Handle the message.
     *
     * @param message   the message to handle
     * @param keepAlive object for notifying the message broker that progress is still being made on the message,
     *                  and that timeout should be reset
     */
    void handle(M message, KeepAlive keepAlive);
}
