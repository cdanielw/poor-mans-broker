package org.openforis.rmb.messagebroker.monitor;

import org.openforis.rmb.messagebroker.MessageProcessingUpdate;

public class MessageUpdateConflictEvent implements Event {
    public final MessageProcessingUpdate<?> update;
    public final Object message;

    public MessageUpdateConflictEvent(MessageProcessingUpdate<?> update, Object message) {
        this.update = update;
        this.message = message;
    }

    public String toString() {
        return "MessageUpdateConflictEvent{" +
                "update=" + update +
                ", message=" + message +
                '}';
    }
}
