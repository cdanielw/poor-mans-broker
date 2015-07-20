package org.openforis.rmb.messagebroker.jdbc;

import org.openforis.rmb.messagebroker.*;
import org.openforis.rmb.messagebroker.MessageProcessingUpdate.Status;
import org.openforis.rmb.messagebroker.util.Clock;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class JdbcMessageRepository implements MessageRepository {
    private final JdbcConnectionManager connectionManager;
    private final String tablePrefix;
    private Clock clock = new Clock.SystemClock();

    public JdbcMessageRepository(JdbcConnectionManager connectionManager, String tablePrefix) {
        this.connectionManager = connectionManager;
        this.tablePrefix = tablePrefix;
    }

    void setClock(Clock clock) {
        this.clock = clock;
    }

    public void add(final String queueId, final List<MessageConsumer<?>> consumers, final Object serializedMessage) {
        withConnection(new ConnectionCallback() {
            public Void execute(Connection connection) throws SQLException {
                String messageId = insertMessage(connection, queueId, serializedMessage);
                insertMessageConsumers(connection, messageId, consumers);
                return null;
            }
        });
    }

    private void insertMessageConsumers(Connection connection, String messageId, List<MessageConsumer<?>> consumers)
            throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "INSERT INTO " + tablePrefix + "message_consumer(message_id, consumer_id, version_id, status, last_updated, times_out, retries)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?)");
        for (MessageConsumer<?> consumer : consumers) {
            long creationTime = clock.millis();
            ps.setString(1, messageId);
            ps.setString(2, consumer.id);
            ps.setString(3, UUID.randomUUID().toString());
            ps.setString(4, Status.PENDING.name());
            ps.setTimestamp(5, new Timestamp(creationTime));
            ps.setTimestamp(6, new Timestamp(timesOut(consumer, creationTime)));
            ps.setInt(7, 0);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
    }

    private String insertMessage(Connection connection, String queueId, Object serializedMessage)
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


    public void take(final Map<MessageConsumer<?>, Integer> maxCountByConsumer, final MessageCallback callback) {
        withConnection(new ConnectionCallback() {
            public Void execute(Connection connection) throws SQLException {
                for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
                    MessageConsumer<?> consumer = entry.getKey();
                    Integer maxCount = entry.getValue();
                    takeMessages(connection, consumer, maxCount, callback);
                }
                return null;
            }
        });
    }

    private void takeMessages(Connection connection, MessageConsumer<?> consumer, int maxCount, MessageCallback callback)
            throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "SELECT queue_id, message_id, publication_time, version_id, status, message_string, message_bytes, " +
                "       times_out, retries, error_message \n" +
                "FROM " + tablePrefix + "message_consumer mc\n" +
                "JOIN " + tablePrefix + "message m ON mc.message_id = m.id\n" +
                "WHERE consumer_id = ?\n" +
                "AND status IN ('PENDING', 'PROCESSING')\n" +
                "ORDER BY sequence_no");
        ps.setString(1, consumer.id);
        ps.setMaxRows(maxCount);
        ResultSet rs = ps.executeQuery();
        while (rs.next())
            if (canTakeMessage(rs))
                takeMessage(connection, rs, consumer, callback);
        rs.close();
        ps.close();
    }

    private boolean canTakeMessage(ResultSet rs) throws SQLException {
        Timestamp now = new Timestamp(clock.millis());
        String status = rs.getString("status");
        Timestamp timesOut = rs.getTimestamp("times_out");

        return status.equals("PENDING") || timesOut.before(now);
    }

    private void takeMessage(Connection connection, ResultSet rs, MessageConsumer<?> consumer, MessageCallback callback)
            throws SQLException {
        String queueId = rs.getString("queue_id");
        Timestamp publicationTime = rs.getTimestamp("publication_time");
        Status fromStatus = Status.valueOf(rs.getString("status"));
        String messageId = rs.getString("message_id");
        String stringMessage = rs.getString("message_string");
        byte[] bytesMessage = rs.getBytes("message_bytes");
        Object serializedMessage = stringMessage == null ? bytesMessage : stringMessage;
        String versionId = rs.getString("version_id");
        int retries = rs.getInt("retries");
        String errorMessage = rs.getString("error_message");
        MessageProcessingUpdate update = MessageProcessingUpdate.create(
                queueId, consumer, messageId, publicationTime.getTime(),
                fromStatus, Status.PROCESSING, retries, errorMessage, versionId
        );
        if (updateMessageProcessing(connection, update))
            callback.messageTaken(update, serializedMessage);
    }

    private boolean updateMessageProcessing(Connection connection, MessageProcessingUpdate update)
            throws SQLException {
        long now = clock.millis();
        PreparedStatement ps = connection.prepareStatement("" +
                "UPDATE " + tablePrefix + "message_consumer\n" +
                "SET status = ?, last_updated = ?, times_out = ?, version_id = ?, retries = ?, error_message = ? \n" +
                "WHERE message_id = ? AND consumer_id = ? AND version_id = ?");
        ps.setString(1, update.toStatus.name());
        ps.setTimestamp(2, new Timestamp(now));
        ps.setTimestamp(3, new Timestamp(timesOut(update.consumer, now)));
        ps.setString(4, update.toVersionId);
        ps.setInt(5, update.retries);
        ps.setString(6, update.errorMessage);
        ps.setString(7, update.messageId);
        ps.setString(8, update.consumer.id);
        ps.setString(9, update.fromVersionId);

        int rowsUpdated = ps.executeUpdate();
        if (rowsUpdated > 1)
            throw new IllegalStateException("More than one row with message_id " + update.messageId);
        boolean updated = rowsUpdated != 0;
        if (updated)
            connection.commit();
        return updated;
    }

    public boolean update(final MessageProcessingUpdate update) {
        return withConnection(new ConnectionCallback<Boolean>() {
            public Boolean execute(Connection connection) throws SQLException {
                return updateMessageProcessing(connection, update);
            }
        });
    }

    public Map<String, Integer> messageQueueSizeByConsumerId() {
        return withConnection(new ConnectionCallback<Map<String, Integer>>() {
            public Map<String, Integer> execute(Connection connection) throws SQLException {
                PreparedStatement ps = connection.prepareStatement("" +
                        "SELECT consumer_id, count(*) queue_size\n" +
                        "FROM " + tablePrefix + "MESSAGE_CONSUMER mc\n" +
                        "WHERE (status = 'PENDING'\n" +
                        "OR (status = 'PROCESSING' AND times_out < ?))\n" +
                        "GROUP BY consumer_id");
                ps.setTimestamp(1, new Timestamp(clock.millis()));
                ResultSet rs = ps.executeQuery();
                Map<String, Integer> sizeByConsumerId = new HashMap<String, Integer>();
                while (rs.next())
                    sizeByConsumerId.put(rs.getString("consumer_id"), rs.getInt("queue_size"));
                rs.close();
                ps.close();
                return sizeByConsumerId;
            }
        });
    }

    private long timesOut(MessageConsumer<?> consumer, long now) {
        return now + consumer.timeUnit.toMillis(consumer.timeout);
    }

    private <T> T withConnection(ConnectionCallback<T> callback) {
        Connection connection = null;
        try {
            connection = connectionManager.getConnection();
            connection.setAutoCommit(false);
            return callback.execute(connection);
        } catch (SQLException e) {
            throw new MessageRepositoryException(e);
        } finally {
            connectionManager.releaseConnection(connection);
        }
    }

    private interface ConnectionCallback<T> {
        T execute(Connection connection) throws SQLException;
    }
}