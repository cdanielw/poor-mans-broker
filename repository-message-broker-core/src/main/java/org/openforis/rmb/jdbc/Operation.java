package org.openforis.rmb.jdbc;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingUpdate;

import java.sql.*;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

abstract class Operation {
    final Connection connection;
    final String tablePrefix;
    final Clock clock;

    Operation(Connection connection, String tablePrefix, Clock clock) {
        this.connection = connection;
        this.tablePrefix = tablePrefix;
        this.clock = clock;
    }

    final long timesOut(MessageConsumer<?> consumer, long now) {
        return now + consumer.getTimeUnit().toMillis(consumer.getTimeout());
    }

    final Object serializedMessage(ResultSet rs) throws SQLException {
        String stringMessage = rs.getString("message_string");
        byte[] bytesMessage = rs.getBytes("message_bytes");
        return stringMessage == null ? bytesMessage : stringMessage;
    }

    final Date toDate(Timestamp timestamp) {
        return new Date(timestamp.getTime());
    }

    final Date now() {
        return new Date(clock.millis());
    }


    final boolean updateMessageProcessing(MessageProcessingUpdate update)
            throws SQLException {
        long now = clock.millis();
        PreparedStatement ps = connection.prepareStatement("" +
                "UPDATE " + tablePrefix + "message_processing\n" +
                "SET state = ?, last_updated = ?, times_out = ?, version_id = ?, retries = ?, error_message = ? \n" +
                "WHERE message_id = ? AND consumer_id = ? AND version_id = ?");
        ps.setString(1, update.getToState().name());
        ps.setTimestamp(2, new Timestamp(now));
        ps.setTimestamp(3, new Timestamp(timesOut(update.getConsumer(), now)));
        ps.setString(4, update.getToVersionId());
        ps.setInt(5, update.getRetries());
        ps.setString(6, update.getErrorMessage());
        ps.setString(7, update.getMessageId());
        ps.setString(8, update.getConsumer().getId());
        ps.setString(9, update.getFromVersionId());

        int rowsUpdated = ps.executeUpdate();
        if (rowsUpdated > 1)
            throw new IllegalStateException("More than one row with message_id " + update.getMessageId());
        connection.commit();
        return rowsUpdated != 0;
    }


    final void deleteOrphanedMessages() throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "DELETE FROM message\n" +
                "WHERE id NOT IN (SELECT message_id FROM message_processing)");
        ps.executeUpdate();
    }

    final Map<String, MessageConsumer<?>> consumersById(Collection<MessageConsumer<?>> consumers) {
        Map<String, MessageConsumer<?>> consumerById = new HashMap<String, MessageConsumer<?>>();
        for (MessageConsumer<?> consumer : consumers)
            consumerById.put(consumer.getId(), consumer);
        return consumerById;
    }
}
