package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageProcessingFilter;

class CountMessages extends FilteringOperation<Integer> {
    public CountMessages(Clock clock, MessageProcessingFilter filter) {
        super(clock, filter);
    }

    Integer execute(ConsumerMessages consumerMessages) {
        return findMessagesForConsumer(consumerMessages).size();
    }
}
