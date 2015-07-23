package org.openforis.rmb.messagebroker.jdbc;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.Clock;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.List;
import java.util.UUID;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.PENDING;

final class MessageAdder extends Operation {
    MessageAdder(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws SQLException {
        String messageId = insertMessage(queueId, serializedMessage);
        insertMessageConsumers(messageId, consumers);
    }

    private void insertMessageConsumers(String messageId, List<MessageConsumer<?>> consumers)
            throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "INSERT INTO " + tablePrefix + "message_processing(message_id, consumer_id, version_id, state, last_updated, times_out, retries)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?)");
        for (MessageConsumer<?> consumer : consumers) {
            long creationTime = clock.millis();
            ps.setString(1, messageId);
            ps.setString(2, consumer.getId());
            ps.setString(3, UUID.randomUUID().toString());
            ps.setString(4, PENDING.name());
            ps.setTimestamp(5, new Timestamp(creationTime));
            ps.setTimestamp(6, new Timestamp(timesOut(consumer, creationTime)));
            ps.setInt(7, 0);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
    }

    private String insertMessage(String queueId, Object serializedMessage)
            throws SQLException {
        String messageId = UUID.randomUUID().toString();
        PreparedStatement ps = connection.prepareStatement("" +
                "INSERT INTO " + tablePrefix + "message(id, publication_time, queue_id, message_string, message_bytes)\n" +
                "VALUES(?, ?, ?, ?, ?)");
        ps.setString(1, messageId);
        ps.setTimestamp(2, new Timestamp(clock.millis()));
        ps.setString(3, queueId);
        if (serializedMessage instanceof String) {
            ps.setString(4, (String) serializedMessage);
            ps.setNull(5, Types.BINARY);
        } else if (serializedMessage instanceof byte[]) {
            ps.setNull(4, Types.VARCHAR);
            ps.setBlob(5, new ByteArrayInputStream((byte[]) serializedMessage));
        } else
            throw new IllegalArgumentException("Support only message serialized to either String or byte[]");
        ps.executeUpdate();
        ps.close();
        return messageId;
    }
}
