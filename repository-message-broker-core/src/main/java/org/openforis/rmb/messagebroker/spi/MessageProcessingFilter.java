package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State;
import org.openforis.rmb.messagebroker.util.Is;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public final class MessageProcessingFilter {
    private final Set<State> states;
    private final Date publishedBefore;
    private final Date publishedAfter;
    private final Date lastUpdatedBefore;
    private final Date lastUpdatedAfter;
    private final Set<String> messageIds;

    private MessageProcessingFilter(Builder builder) {
        this.states = builder.states;
        this.publishedBefore = builder.publishedBefore;
        this.publishedAfter = builder.publishedAfter;
        this.lastUpdatedBefore = builder.lastUpdatedBefore;
        this.lastUpdatedAfter = builder.lastUpdatedAfter;
        this.messageIds = builder.messageIds;
    }

    public static Builder builder() {
        return new Builder();
    }

    public Set<State> getStates() {
        return states;
    }

    public Date getPublishedBefore() {
        return publishedBefore;
    }

    public Date getPublishedAfter() {
        return publishedAfter;
    }

    public Date getLastUpdatedBefore() {
        return lastUpdatedBefore;
    }

    public Date getLastUpdatedAfter() {
        return lastUpdatedAfter;
    }

    public Set<String> getMessageIds() {
        return messageIds;
    }

    public static class Builder {
        private Set<State> states = new HashSet<State>();
        private Date publishedBefore;
        private Date publishedAfter;
        private Date lastUpdatedBefore;
        private Date lastUpdatedAfter;
        private Set<String> messageIds = new HashSet<String>();

        private Builder() { }

        public Builder states(State... states) {
            Collections.addAll(this.states, states);
            return this;
        }

        public Builder publishedBetween(Date from, Date to) {
            Is.notNull(from, "from must not be null");
            Is.notNull(to, "to must not be null");
            publishedAfter = from;
            publishedBefore = to;
            return this;
        }

        public Builder publishedBefore(Date date) {
            Is.notNull(date, "date must not be null");
            publishedBefore = date;
            return this;
        }

        public Builder publishedAfter(Date date) {
            Is.notNull(date, "date must not be null");
            publishedAfter = date;
            return this;
        }

        public Builder lastUpdatedBetween(Date from, Date to) {
            Is.notNull(from, "from must not be null");
            Is.notNull(to, "to must not be null");
            lastUpdatedAfter = from;
            lastUpdatedBefore = to;
            return this;
        }

        public Builder lastUpdatedBefore(Date date) {
            Is.notNull(date, "date must not be null");
            lastUpdatedBefore = date;
            return this;
        }

        public Builder lastUpdatedAfter(Date date) {
            Is.notNull(date, "date must not be null");
            lastUpdatedAfter = date;
            return this;
        }

        public Builder messageIds(String... messageIds) {
            Collections.addAll(this.messageIds, messageIds);
            return this;
        }

        public MessageProcessingFilter build() {
            return new MessageProcessingFilter(this);
        }
    }
}
