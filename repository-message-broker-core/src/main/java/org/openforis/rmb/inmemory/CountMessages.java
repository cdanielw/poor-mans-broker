package org.openforis.rmb.inmemory;

import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingFilter;

class CountMessages extends FilteringOperation<Integer> {
    public CountMessages(Clock clock, MessageProcessingFilter filter) {
        super(clock, filter);
    }

    Integer execute(ConsumerMessages consumerMessages) {
        return findMessagesForConsumer(consumerMessages).size();
    }
}
