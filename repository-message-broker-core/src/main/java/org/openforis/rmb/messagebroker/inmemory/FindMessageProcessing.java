package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.spi.*;
import org.openforis.rmb.messagebroker.spi.MessageRepository.MessageProcessingFoundCallback;

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
                    new MessageDetails(update.queueId, update.messageId, update.publicationTime),
                    consumerMessages.consumer,
                    new MessageProcessingStatus(update.toState, update.retries, update.errorMessage, now(), update.toVersionId)
            ), message.serializedMessage);
        }
        return null;
    }

}
