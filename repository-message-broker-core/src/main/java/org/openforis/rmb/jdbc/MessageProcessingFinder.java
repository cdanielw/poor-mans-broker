package org.openforis.rmb.jdbc;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.TIMED_OUT;
import static org.openforis.rmb.spi.MessageProcessingStatus.State.valueOf;

final class MessageProcessingFinder extends Operation {
    MessageProcessingFinder(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    void find(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter, MessageRepository.MessageProcessingFoundCallback callback) throws SQLException {
        ConstraintBuilder constraintBuilder = new ConstraintBuilder(consumers, filter, clock);
        PreparedStatement ps = connection.prepareStatement("" +
                "SELECT consumer_id, queue_id, message_id, publication_time, times_out, state, retries, " +
                "       error_message, version_id, message_string, message_bytes\n" +
                "FROM " + tablePrefix + "message_processing mc\n" +
                "JOIN " + tablePrefix + "message m ON mc.message_id = m.id\n" +
                "WHERE " + constraintBuilder.whereClause() + "\n" +
                "ORDER BY sequence_no, consumer_id");

        constraintBuilder.bind(ps);

        ResultSet rs = ps.executeQuery();
        Map<String, MessageConsumer<?>> consumerById = consumersById(consumers);

        while (rs.next())
            callback.found(messageProcessing(rs, consumerById), serializedMessage(rs));
        rs.close();
        ps.close();
    }

    private MessageProcessing<?> messageProcessing(ResultSet rs, Map<String, MessageConsumer<?>> consumerById)
            throws SQLException {
        String consumerId = rs.getString("consumer_id");
        String queueId = rs.getString("queue_id");
        String messageId = rs.getString("message_id");
        Date publicationTime = toDate(rs.getTimestamp("publication_time"));
        MessageProcessingStatus.State state = state(rs);
        int retries = rs.getInt("retries");
        String errorMessage = rs.getString("error_message");
        String versionId = rs.getString("version_id");

        return MessageProcessing.create(
                new MessageDetails(queueId, messageId, publicationTime),
                consumerById.get(consumerId),
                new MessageProcessingStatus(state, retries, errorMessage, now(), versionId)
        );
    }

    private MessageProcessingStatus.State state(ResultSet rs) throws SQLException {
        long timesOut = rs.getTimestamp("times_out").getTime();
        return timesOut < clock.millis() ? TIMED_OUT : valueOf(rs.getString("state"));
    }
}
