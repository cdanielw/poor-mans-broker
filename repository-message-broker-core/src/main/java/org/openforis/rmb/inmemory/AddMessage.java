package org.openforis.rmb.inmemory;

import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageDetails;
import org.openforis.rmb.spi.MessageProcessingStatus;
import org.openforis.rmb.spi.MessageProcessingUpdate;

import java.util.Date;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.PENDING;

final class AddMessage extends InMemoryDatabase.Operation<Void> {
    private final String queueId;
    private final Object serializedMessage;

    public AddMessage(Clock clock, String queueId, Object serializedMessage) {
        super(clock);
        this.queueId = queueId;
        this.serializedMessage = serializedMessage;
    }

    Void execute(ConsumerMessages consumerMessages) {
        String messageId = randomUuid();
        Date publicationTime = now();
        MessageProcessingUpdate<?> update = MessageProcessingUpdate
                .create(
                        new MessageDetails(queueId, messageId, publicationTime),
                        consumerMessages.consumer,
                        new MessageProcessingStatus(PENDING, 0, null, now(), randomUuid()),
                        new MessageProcessingStatus(PENDING, 0, null, now(), randomUuid())
                );


        consumerMessages.add(new Message(clock, update, serializedMessage));
        return null;
    }
}
