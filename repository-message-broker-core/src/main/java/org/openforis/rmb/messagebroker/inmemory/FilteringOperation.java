package org.openforis.rmb.messagebroker.inmemory;

import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageProcessingFilter;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.TIMED_OUT;

public abstract class FilteringOperation<T> extends InMemoryDatabase.Operation<T> {
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
        if (!filter.states.isEmpty()) {
            boolean notMatchingState = !filter.states.contains(update.toState);
            boolean notMatchingTimedOut = !filter.states.contains(TIMED_OUT) || !message.timedOut();
            if (notMatchingState && notMatchingTimedOut)
                return false;
        }
        if (filter.publishedBefore != null && !update.publicationTime.before(filter.publishedBefore))
            return false;
        if (filter.publishedAfter != null && !update.publicationTime.after(filter.publishedAfter))
            return false;
        if (filter.lastUpdatedBefore != null && !update.updateTime.before(filter.lastUpdatedBefore))
            return false;
        if (filter.lastUpdatedAfter != null && !update.updateTime.after(filter.lastUpdatedAfter))
            return false;
        if (!filter.messageIds.isEmpty() && !filter.messageIds.contains(message.update.messageId))
            return false;
        return true;
    }
}
