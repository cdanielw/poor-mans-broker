package org.openforis.rmb.inmemory;


import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageDetails;
import org.openforis.rmb.spi.MessageProcessingStatus;
import org.openforis.rmb.spi.MessageProcessingUpdate;

import java.util.Date;
import java.util.UUID;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.*;

final class Message {
    private final Clock clock;
    MessageProcessingUpdate update;
    Date timesOut;
    final Object serializedMessage;

    Message(Clock clock, MessageProcessingUpdate update, Object serializedMessage) {
        this.clock = clock;
        this.update = update;
        long timeoutMillis = update.getConsumer().getTimeUnit().toMillis(update.getConsumer().getTimeout());
        this.timesOut = new Date(update.getPublicationTime().getTime() + timeoutMillis);
        this.serializedMessage = serializedMessage;
    }

    void take() {
        MessageProcessingStatus.State fromState = timedOut() ? TIMED_OUT : update.getToState();
        setUpdate(MessageProcessingUpdate.create(
                new MessageDetails(update.getQueueId(), update.getMessageId(), update.getPublicationTime()), update.getConsumer(),
                new MessageProcessingStatus(fromState, update.getRetries(), update.getErrorMessage(), now(), update.getToVersionId()),
                new MessageProcessingStatus(PROCESSING, update.getRetries(), update.getErrorMessage(), now(), randomUuid())
        ));
    }

    void setUpdate(MessageProcessingUpdate update) {
        this.update = update;
    }

    boolean isPending() {
        return update.getToState() == PENDING;
    }

    boolean timedOut() {
        return update.getToState() == PROCESSING && timesOut.before(now());
    }

    boolean isCompleted() {
        return update.getToState() == COMPLETED;
    }

    private Date now() {
        return new Date(clock.millis());
    }

    private String randomUuid() {
        return UUID.randomUUID().toString();
    }
}
