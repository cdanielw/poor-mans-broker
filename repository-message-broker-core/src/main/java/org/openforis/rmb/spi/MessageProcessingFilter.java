package org.openforis.rmb.spi;

import org.openforis.rmb.spi.MessageProcessingStatus.State;
import org.openforis.rmb.util.Is;

import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 * Specifies a selection of message processing.
 * <p>
 * Instances are created through {@link #builder()}.
 * </p>
 * <p>
 * This class is immutable.
 * </p>
 */
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

    /**
     * Provides a builder for creating {@link MessageProcessingFilter}.
     *
     * @return the builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets the states to include. If empty, all states are included.
     *
     * @return the states to include
     */
    public Set<State> getStates() {
        return states;
    }

    /**
     * Gets the time the message must have been published before. If null, constraint is not applied.
     *
     * @return the time the message must have been published before
     */
    public Date getPublishedBefore() {
        return publishedBefore;
    }

    /**
     * Gets the time the message must have been published after. If null, constraint is not applied.
     *
     * @return time the message must have been published after
     */
    public Date getPublishedAfter() {
        return publishedAfter;
    }

    /**
     * Gets the time the message processing must have been updated before. If null, constraint is not applied.
     *
     * @return the time the message processing must have been updated before
     */
    public Date getLastUpdatedBefore() {
        return lastUpdatedBefore;
    }

    /**
     * Gets the time the message processing must have been updated after. If null, constraint is not applied.
     *
     * @return the time the message processing must have been updated after
     */
    public Date getLastUpdatedAfter() {
        return lastUpdatedAfter;
    }

    /**
     * Gets the message ids to limit the selection to. If empty, all messages are included.
     *
     * @return the message ids to limit the selection to
     */
    public Set<String> getMessageIds() {
        return messageIds;
    }

    /**
     * Buildes {@link MessageProcessingFilter} instances. Configure the {@link MessageProcessingFilter} through
     * the chainable builder methods, and finally build using {@link #build()}.
     * <p>
     * Instances are created though {@link #builder()}.
     * </p>
     */
    public static final class Builder {
        private Set<State> states = new HashSet<State>();
        private Date publishedBefore;
        private Date publishedAfter;
        private Date lastUpdatedBefore;
        private Date lastUpdatedAfter;
        private Set<String> messageIds = new HashSet<String>();

        private Builder() { }

        /**
         * Limit the selection to specified states.
         *
         * @param states the states to limit the selection to
         * @return the builder, so methods can be chained
         */
        public Builder states(State... states) {
            Collections.addAll(this.states, states);
            return this;
        }

        /**
         * Limits the message publication time to specified, exclusive, range.
         *
         * @param from messages must have been published after this time
         * @param to   messages must have been published before this time
         * @return the builder, so methods can be chained
         */
        public Builder publishedBetween(Date from, Date to) {
            Is.notNull(from, "from must not be null");
            Is.notNull(to, "to must not be null");
            publishedAfter = from;
            publishedBefore = to;
            return this;
        }

        /**
         * Limits the message publication time to messages published before the specified time.
         *
         * @param time messages must have been published before this time
         * @return the builder, so methods can be chained
         */
        public Builder publishedBefore(Date time) {
            Is.notNull(time, "time must not be null");
            publishedBefore = time;
            return this;
        }

        /**
         * Limits the message publication time to messages published after the specified time.
         *
         * @param time messages must have been published after this time
         * @return the builder, so methods can be chained
         */
        public Builder publishedAfter(Date time) {
            Is.notNull(time, "time must not be null");
            publishedAfter = time;
            return this;
        }

        /**
         * Limits the message processing update time to the specified, exclusive, range.
         *
         * @param from message processing must have been updated after this time
         * @param to   message processing must have been updated before this time
         * @return the builder, so methods can be chained
         */
        public Builder lastUpdatedBetween(Date from, Date to) {
            Is.notNull(from, "from must not be null");
            Is.notNull(to, "to must not be null");
            lastUpdatedAfter = from;
            lastUpdatedBefore = to;
            return this;
        }

        /**
         * Limits the message processing update time to those updated before specified time.
         *
         * @param date message processing must have been updated before this time
         * @return the builder, so methods can be chained
         */
        public Builder lastUpdatedBefore(Date date) {
            Is.notNull(date, "date must not be null");
            lastUpdatedBefore = date;
            return this;
        }

        /**
         * Limits the message processing update time to those updated after specified time.
         *
         * @param date message processing must have been updated after this time
         * @return the builder, so methods can be chained
         */
        public Builder lastUpdatedAfter(Date date) {
            Is.notNull(date, "date must not be null");
            lastUpdatedAfter = date;
            return this;
        }

        /**
         * Limits the message ids to include.
         *
         * @param messageIds the message ids to include
         * @return the builder, so methods can be chained
         */
        public Builder messageIds(String... messageIds) {
            Collections.addAll(this.messageIds, messageIds);
            return this;
        }

        /**
         * Builds the {@link MessageProcessingFilter}, based on how the builder's been configured.
         *
         * @return the filter instance
         */
        public MessageProcessingFilter build() {
            return new MessageProcessingFilter(this);
        }
    }
}
