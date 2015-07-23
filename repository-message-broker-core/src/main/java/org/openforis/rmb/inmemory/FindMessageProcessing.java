package org.openforis.rmb.inmemory;

import org.openforis.rmb.spi.*;
import org.openforis.rmb.spi.MessageRepository.MessageProcessingFoundCallback;

import java.util.List;

final class FindMessageProcessing extends FilteringOperation<Void> {
    private final MessageProcessingFoundCallback callback;

    public FindMessageProcessing(Clock clock, MessageProcessingFilter filter, MessageProcessingFoundCallback callback) {
        super(clock, filter);
        this.callback = callback;
    }

    Void execute(ConsumerMessages consumerMessages) {
        List<Message> messages = findMessagesForConsumer(consumerMessages);
        for (Message message : messages) {
            MessageProcessingUpdate update = message.update;
            callback.found(MessageProcessing.create(
                    new MessageDetails(update.getQueueId(), update.getMessageId(), update.getPublicationTime()),
                    consumerMessages.consumer,
                    new MessageProcessingStatus(update.getToState(), update.getRetries(), update.getErrorMessage(),
                            now(), update.getToVersionId())
            ), message.serializedMessage);
        }
        return null;
    }

}
