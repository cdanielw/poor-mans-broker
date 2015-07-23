package org.openforis.rmb.messagebroker.inmemory;


import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageDetails;
import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;

import java.util.Date;
import java.util.UUID;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*;

final class Message {
    private final Clock clock;
    MessageProcessingUpdate update;
    Date timesOut;
    final Object serializedMessage;

    Message(Clock clock, MessageProcessingUpdate update, Object serializedMessage) {
        this.clock = clock;
        this.update = update;
        long timeoutMillis = update.consumer.timeUnit.toMillis(update.consumer.timeout);
        this.timesOut = new Date(update.publicationTime.getTime() + timeoutMillis);
        this.serializedMessage = serializedMessage;
    }

    void take() {
        MessageProcessingStatus.State fromState = timedOut() ? TIMED_OUT : update.toState;
        setUpdate(MessageProcessingUpdate.create(
                new MessageDetails(update.queueId, update.messageId, update.publicationTime), update.consumer,
                new MessageProcessingStatus(fromState, update.retries, update.errorMessage, now(), update.toVersionId),
                new MessageProcessingStatus(PROCESSING, update.retries, update.errorMessage, now(), randomUuid())
        ));
    }

    void setUpdate(MessageProcessingUpdate update) {
        this.update = update;
    }

    boolean isPending() {
        return update.toState == PENDING;
    }

    boolean timedOut() {
        return update.toState == PROCESSING && timesOut.before(now());
    }

    boolean isCompleted() {
        return update.toState == COMPLETED;
    }

    private Date now() {
        return new Date(clock.millis());
    }

    private String randomUuid() {
        return UUID.randomUUID().toString();
    }
}
