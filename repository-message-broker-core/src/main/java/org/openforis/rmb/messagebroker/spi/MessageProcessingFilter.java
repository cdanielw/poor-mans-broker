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
    public final Date lastChangedBefore;
    public final Date lastChangedAfter;
    public final Set<String> messageIds;

    public MessageProcessingFilter(Builder builder) {
        this.states = builder.states;
        this.publishedBefore = builder.publishedBefore;
        this.publishedAfter = builder.publishedAfter;
        this.lastChangedBefore = builder.lastChangedBefore;
        this.lastChangedAfter = builder.lastChangedAfter;
        this.messageIds = builder.messageIds;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Set<State> states = new HashSet<State>();
        private Date publishedBefore;
        private Date publishedAfter;
        private Date lastChangedBefore;
        private Date lastChangedAfter;
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

        public Builder lastChangedBetween(Date from, Date to) {
            lastChangedAfter = from;
            lastChangedBefore = to;
            return this;
        }

        public Builder lastChangedBefore(Date date) {
            lastChangedBefore = date;
            return this;
        }

        public Builder lastChangedAfter(Date date) {
            lastChangedAfter = date;
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
