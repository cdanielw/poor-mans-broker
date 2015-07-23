package org.openforis.rmb.inmemory;

import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingFilter;
import org.openforis.rmb.spi.MessageProcessingUpdate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.TIMED_OUT;

abstract class FilteringOperation<T> extends InMemoryDatabase.Operation<T> {
    final MessageProcessingFilter filter;

    public FilteringOperation(Clock clock, MessageProcessingFilter filter) {
        super(clock);
        this.filter = filter;
    }

    protected List<Message> findMessagesForConsumer(ConsumerMessages consumerMessages) {
        if (consumerMessages == null)
            return Collections.emptyList();
        List<Message> messages = new ArrayList<Message>();
        for (Message message : consumerMessages.messages)
            if (include(message))
                messages.add(message);
        return messages;
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean include(Message message) {
        MessageProcessingUpdate update = message.update;
        if (!filter.getStates().isEmpty()) {
            boolean notMatchingState = !filter.getStates().contains(update.getToState());
            boolean notMatchingTimedOut = !filter.getStates().contains(TIMED_OUT) || !message.timedOut();
            if (notMatchingState && notMatchingTimedOut)
                return false;
        }
        if (filter.getPublishedBefore() != null && !update.getPublicationTime().before(filter.getPublishedBefore()))
            return false;
        if (filter.getPublishedAfter() != null && !update.getPublicationTime().after(filter.getPublishedAfter()))
            return false;
        if (filter.getLastUpdatedBefore() != null && !update.getUpdateTime().before(filter.getLastUpdatedBefore()))
            return false;
        if (filter.getLastUpdatedAfter() != null && !update.getUpdateTime().after(filter.getLastUpdatedAfter()))
            return false;
        if (!filter.getMessageIds().isEmpty() && !filter.getMessageIds().contains(message.update.getMessageId()))
            return false;
        return true;
    }
}
