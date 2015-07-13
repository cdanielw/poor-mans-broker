package com.wiell.messagebroker.jdbc;

import com.wiell.messagebroker.MessageConsumer;
import com.wiell.messagebroker.spi.MessageCallback;
import com.wiell.messagebroker.spi.MessageRepository;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class JdbcMessageRepository implements MessageRepository {
    private final JdbcConnectionManager connectionManager;

    public JdbcMessageRepository(JdbcConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public void addMessage(final String queueId, final List<MessageConsumer<?>> consumers, final String serializedMessage) {
        withConnection(new ConnectionCallback() {
            public void execute(Connection connection) throws SQLException {
                String messageId = insertMessage(connection, queueId, serializedMessage);
                insertMessageConsumers(connection, messageId, consumers);
            }
        });
    }

    private void insertMessageConsumers(Connection connection, String messageId, List<MessageConsumer<?>> consumers) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "INSERT INTO message_consumer(message_id, consumer_id, version_id, status, last_updated, times_out, retries)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?)");
        for (MessageConsumer<?> consumer : consumers) {
            long creationTime = System.currentTimeMillis();
            ps.setString(1, messageId);
            ps.setString(2, consumer.id);
            ps.setInt(3, 1);
            ps.setString(4, Status.PENDING.name());
            ps.setTimestamp(5, new Timestamp(creationTime));
            ps.setTimestamp(6, new Timestamp(timesOut(consumer, creationTime)));
            ps.setInt(7, 0);
            ps.addBatch();
        }
        ps.executeBatch();
        ps.close();
    }

    private String insertMessage(Connection connection, String queueId, String serializedMessage) throws SQLException {
        String messageId = UUID.randomUUID().toString();
        PreparedStatement ps = connection.prepareStatement("" +
                "INSERT INTO message(id, published, queue_id, message)\n" +
                "VALUES(?, ?, ?, ?)");
        ps.setString(1, messageId);
        ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        ps.setString(3, queueId);
        ps.setString(4, serializedMessage);
        ps.executeUpdate();
        ps.close();
        return messageId;
    }


    public void takeMessages(final Map<MessageConsumer<?>, Integer> maxCountByConsumer, final MessageCallback callback) {
        withConnection(new ConnectionCallback() {
            public void execute(Connection connection) throws SQLException {
                for (Map.Entry<MessageConsumer<?>, Integer> entry : maxCountByConsumer.entrySet()) {
                    MessageConsumer<?> consumer = entry.getKey();
                    Integer maxCount = entry.getValue();
                    takeMessages(connection, consumer, maxCount, callback);
                }
            }
        });
    }


    private void takeMessages(Connection connection, MessageConsumer<?> consumer, Integer maxCount, MessageCallback callback) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "SELECT message_id, version_id, status, message \n" +
                "FROM message_consumer mc\n" +
                "JOIN message m ON mc.message_id = m.id\n" +
                "WHERE consumer_id = ?\n" +
                "AND (status = 'PENDING'\n" +
                "OR (status = 'PROCESSING' AND times_out > ?))");
        ps.setString(1, consumer.id);
        ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        ps.setMaxRows(maxCount);
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            Status status = Status.valueOf(rs.getString("status"));
            String messageId = rs.getString("message_id");
            String serializedMessage = rs.getString("message");
            int versionId = rs.getInt("version_id");

            if (updateStatus(connection, consumer, messageId, versionId, Status.PROCESSING))
                callback.messageTaken(consumer, messageId, serializedMessage); // TODO: Pass the status too
        }
        rs.close();
        ps.close();
    }

    // TODO: Some of these args should be grouped into a class. To be used in the callback too...

    private boolean updateStatus(Connection connection, MessageConsumer<?> consumer, String messageId, int versionId, Status status) throws SQLException {
        long now = System.currentTimeMillis();
        PreparedStatement ps = connection.prepareStatement("" +
                "UPDATE message_consumer SET status = ?, last_updated = ?, times_out = ?, version_id = ?\n" +
                "WHERE message_id = ? AND version_id = ?");
        ps.setString(1, status.name());
        ps.setTimestamp(2, new Timestamp(now));
        ps.setTimestamp(3, new Timestamp(timesOut(consumer, now)));
        ps.setInt(4, versionId + 1);
        ps.setString(5, messageId);
        ps.setInt(6, versionId);

        int rowsUpdated = ps.executeUpdate();
        if (rowsUpdated > 1)
            throw new IllegalStateException("More than one row with message_id " + messageId);
        connection.commit();
        return rowsUpdated != 0;
    }

    public void keepAlive(final MessageConsumer<?> consumer, final String messageId) {
        withConnection(new ConnectionCallback() {
            public void execute(Connection connection) throws SQLException {
                updateStatus(connection, consumer, messageId, -1, Status.PROCESSING); // TODO: Version ID needs to be passed around?
            }
        });
        // TODO: Could update the status to PROCESSING - even if that's already it
    }

    public void completed(MessageConsumer<?> consumer, String messageId) {
        // TODO: Could update the status to COMPLETED
    }

    public void retrying(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) {
    }

    public void failed(MessageConsumer<?> consumer, String messageId, int retries, Exception exception) {
    }

    private long timesOut(MessageConsumer<?> consumer, long now) {return now + consumer.timeUnit.toMillis(consumer.timeout);}

    private void withConnection(ConnectionCallback callback) {
        Connection connection = null;
        try {
            connection = connectionManager.getConnection();
            connection.setAutoCommit(false);
            callback.execute(connection);
        } catch (SQLException e) {
            throw new SqlExceptionTranslator(e).translate();
        } finally {
            try {
                connectionManager.releaseConnection(connection);
            } catch (SQLException e) {
                // TODO: Think of the event to throw
            }
        }
    }

    private interface ConnectionCallback {
        void execute(Connection connection) throws SQLException;
    }

    private enum Status {
        PENDING, PROCESSING, COMPLETED, FAILED
    }
/*

 ---------------------------
| prefix_message_processing |
|---------------------------|
| message_id    pk          |
| consumer_id   pk          |
| version                   |
| status                    |
| last_updated not null     |
| timeout      not null     |
| retries      not null     |
| error                     |
 ---------------------------

 ----------------------------
| prefix_message             |
|----------------------------|
| id        pk autoincrement |
| published not null         |
| queue_id  not null         |
| message   not null         |
 ----------------------------


*/
}
