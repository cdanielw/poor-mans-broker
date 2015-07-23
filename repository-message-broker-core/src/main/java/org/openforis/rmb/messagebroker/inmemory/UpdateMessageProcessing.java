package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;

class UpdateMessageProcessing extends InMemoryDatabase.Operation<Boolean> {
    private final MessageProcessingUpdate update;

    public UpdateMessageProcessing(Clock clock, MessageProcessingUpdate update) {
        super(clock);
        this.update = update;
    }

    Boolean execute(ConsumerMessages consumerMessages) {
        consumerMessages.apply(update);
        return true;
    }
}
