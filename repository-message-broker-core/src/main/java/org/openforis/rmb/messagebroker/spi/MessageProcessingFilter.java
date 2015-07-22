package org.openforis.rmb.messagebroker.spi;

import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

public final class MessageProcessingFilter {
    public final Set<State> states;
    public final Date publishedBefore;
    public final Date publishedAfter;
    public final Date lastUpdatedBefore;
    public final Date lastUpdatedAfter;
    public final Set<String> messageIds;

    public MessageProcessingFilter(Builder builder) {
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
            publishedAfter = from;
            publishedBefore = to;
            return this;
        }

        public Builder publishedBefore(Date date) {
            publishedBefore = date;
            return this;
        }

        public Builder publishedAfter(Date date) {
            publishedAfter = date;
            return this;
        }

        public Builder lastUpdatedBetween(Date from, Date to) {
            lastUpdatedAfter = from;
            lastUpdatedBefore = to;
            return this;
        }

        public Builder lastUpdatedBefore(Date date) {
            lastUpdatedBefore = date;
            return this;
        }

        public Builder lastUpdatedAfter(Date date) {
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
