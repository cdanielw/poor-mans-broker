package org.openforis.rmb;

/**
 * Allows the message broker to be notified that a consumer still is making progress on a message.
 */
public interface KeepAlive {
    /**
     * Notify the message broker that progress on current message is still being made, and timeout should be reset.
     */
    void send();
}
