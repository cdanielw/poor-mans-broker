package org.openforis.rmb;

/**
 * A simple message handler.
 *
 * @param <M> the type of messages to handle
 */
public interface MessageHandler<M> {
    /**
     * Handle message.
     *
     * @param message the message to handle
     */
    void handle(M message);
}
