package org.openforis.rmb.messagebroker.jdbc;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.*;

import java.sql.*;
import java.util.Map;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.PENDING;
import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.TIMED_OUT;

final class MessageTaker extends Operation {
    MessageTaker(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageRepository.MessageTakenCallback callback) throws SQLException {
        for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
            MessageConsumer<?> consumer = entry.getKey();
            Integer maxCount = entry.getValue();
            takeMessages(consumer, maxCount, callback);
        }
    }

    private void takeMessages(MessageConsumer<?> consumer, int maxCount, MessageRepository.MessageTakenCallback callback)
            throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "SELECT queue_id, message_id, publication_time, version_id, state, message_string, message_bytes, " +
                "       times_out, retries, error_message \n" +
                "FROM " + tablePrefix + "message_processing mc\n" +
                "JOIN " + tablePrefix + "message m ON mc.message_id = m.id\n" +
                "WHERE consumer_id = ?\n" +
                "AND state IN ('PENDING', 'PROCESSING')\n" +
                "ORDER BY sequence_no");
        ps.setString(1, consumer.getId());
        ps.setMaxRows(maxCount);
        ResultSet rs = ps.executeQuery();
        while (rs.next())
            if (canTakeMessage(rs))
                takeMessage(rs, consumer, callback);
        rs.close();
        ps.close();
    }

    private boolean canTakeMessage(ResultSet rs) throws SQLException {
        Timestamp now = new Timestamp(clock.millis());
        String state = rs.getString("state");
        Timestamp timesOut = rs.getTimestamp("times_out");

        return state.equals("PENDING") || timesOut.before(now);
    }

    private void takeMessage(ResultSet rs, MessageConsumer<?> consumer, MessageRepository.MessageTakenCallback callback)
            throws SQLException {
        String queueId = rs.getString("queue_id");
        Timestamp publicationTime = rs.getTimestamp("publication_time");
        MessageProcessingStatus.State fromState = rs.getString("state").equals("PROCESSING") ? TIMED_OUT : PENDING;
        String messageId = rs.getString("message_id");
        Object serializedMessage = serializedMessage(rs);
        String versionId = rs.getString("version_id");
        int retries = rs.getInt("retries");
        String errorMessage = rs.getString("error_message");
        MessageProcessingUpdate update = MessageProcessing.create(
                new MessageDetails(queueId, messageId, toDate(publicationTime)),
                consumer,
                new MessageProcessingStatus(fromState, retries, errorMessage, now(), versionId)
        ).take(clock);
        if (updateMessageProcessing(update))
            callback.taken(update, serializedMessage);
    }
}
