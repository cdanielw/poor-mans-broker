package org.openforis.rmb.inmemory;

import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingFilter;

final class DeleteMessageProcessing extends FilteringOperation<Void> {
    public DeleteMessageProcessing(Clock clock, MessageProcessingFilter filter) {
        super(clock, filter);
    }

    Void execute(ConsumerMessages consumerMessages) {
        for (Message message : findMessagesForConsumer(consumerMessages))
            consumerMessages.remove(message);
        return null;
    }
}
