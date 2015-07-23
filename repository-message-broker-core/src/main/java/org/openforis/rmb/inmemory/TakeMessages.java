package org.openforis.rmb.inmemory;

import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageRepository.MessageTakenCallback;

class TakeMessages extends InMemoryDatabase.Operation<Void> {
    private final Integer maxCount;
    private final MessageTakenCallback callback;

    public TakeMessages(Clock clock, Integer maxCount, MessageTakenCallback callback) {
        super(clock);
        this.maxCount = maxCount;
        this.callback = callback;
    }

    Void execute(ConsumerMessages consumerMessages) {
        consumerMessages.takePending(maxCount, callback);
        return null;
    }
}
