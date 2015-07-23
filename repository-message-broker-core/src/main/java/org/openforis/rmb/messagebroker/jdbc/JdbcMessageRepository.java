package org.openforis.rmb.messagebroker.jdbc;

import org.openforis.rmb.messagebroker.MessageConsumer;
import org.openforis.rmb.messagebroker.spi.*;
import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State;
import org.openforis.rmb.messagebroker.util.Is;

import java.io.ByteArrayInputStream;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*;

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
                "INSERT INTO " + tablePrefix + "message_processing(message_id, consumer_id, version_id, state, last_updated, times_out, retries)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?)");
        for (MessageConsumer<?> consumer : consumers) {
            long creationTime = clock.millis();
            ps.setString(1, messageId);
            ps.setString(2, consumer.id);
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


    public void take(final Map<MessageConsumer<?>, Integer> maxCountByConsumer, final MessageTakenCallback callback) {
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


    private void takeMessages(Connection connection, MessageConsumer<?> consumer, int maxCount, MessageTakenCallback callback)
            throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "SELECT queue_id, message_id, publication_time, version_id, state, message_string, message_bytes, " +
                "       times_out, retries, error_message \n" +
                "FROM " + tablePrefix + "message_processing mc\n" +
                "JOIN " + tablePrefix + "message m ON mc.message_id = m.id\n" +
                "WHERE consumer_id = ?\n" +
                "AND state IN ('PENDING', 'PROCESSING')\n" +
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
        String state = rs.getString("state");
        Timestamp timesOut = rs.getTimestamp("times_out");

        return state.equals("PENDING") || timesOut.before(now);
    }

    private void takeMessage(Connection connection, ResultSet rs, MessageConsumer<?> consumer, MessageTakenCallback callback)
            throws SQLException {
        String queueId = rs.getString("queue_id");
        Timestamp publicationTime = rs.getTimestamp("publication_time");
        State fromState = rs.getString("state").equals("PROCESSING") ? TIMED_OUT : PENDING;
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
        if (updateMessageProcessing(connection, update))
            callback.taken(update, serializedMessage);
    }

    private boolean updateMessageProcessing(Connection connection, MessageProcessingUpdate update)
            throws SQLException {
        long now = clock.millis();
        PreparedStatement ps = connection.prepareStatement("" +
                "UPDATE " + tablePrefix + "message_processing\n" +
                "SET state = ?, last_updated = ?, times_out = ?, version_id = ?, retries = ?, error_message = ? \n" +
                "WHERE message_id = ? AND consumer_id = ? AND version_id = ?");
        ps.setString(1, update.toState.name());
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
        boolean success = rowsUpdated != 0;
        if (success)
            connection.commit();
        return success;
    }

    public boolean update(final MessageProcessingUpdate update) {
        return withConnection(new ConnectionCallback<Boolean>() {
            public Boolean execute(Connection connection) throws SQLException {
                return update.toState == COMPLETED
                        ? deleteCompleted(connection, update)
                        : updateMessageProcessing(connection, update);
            }
        });
    }

    private boolean deleteCompleted(Connection connection, MessageProcessingUpdate update) throws SQLException {
        boolean success = deleteFromMessageConsumer(connection, update);
        if (success) {
            deleteOrphanedMessages(connection, update);
            connection.commit();
        }
        return success;
    }

    private boolean deleteFromMessageConsumer(Connection connection, MessageProcessingUpdate update) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "DELETE FROM message_processing\n" +
                "WHERE message_id = ? AND consumer_id = ? AND version_id = ?");
        ps.setString(1, update.messageId);
        ps.setString(2, update.consumer.id);
        ps.setString(3, update.fromVersionId);
        int rowsDeleted = ps.executeUpdate();
        if (rowsDeleted > 1)
            throw new IllegalStateException("More than one row with message_id " + update.messageId);
        return rowsDeleted != 0;
    }

    private void deleteOrphanedMessages(Connection connection, MessageProcessingUpdate update) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("" +
                "DELETE FROM message\n" +
                "WHERE id = ?\n" +
                "AND id NOT IN (SELECT message_id FROM message_processing)");
        ps.setString(1, update.messageId);
        ps.executeUpdate();
    }

    public void findMessageProcessing(final Collection<MessageConsumer<?>> consumers,
                                      final MessageProcessingFilter filter,
                                      final MessageProcessingFoundCallback callback) {
        Is.notEmpty(consumers, "Must provide at least one consumer");
        Is.notNull(filter, "Filter cannot be null");
        Is.notNull(callback, "Callback cannot be null");
        withConnection(new ConnectionCallback<Void>() {
            public Void execute(Connection connection) throws SQLException {
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

                while (rs.next()) {
                    String consumerId = rs.getString("consumer_id");
                    String queueId = rs.getString("queue_id");
                    String messageId = rs.getString("message_id");
                    Date publicationTime = toDate(rs.getTimestamp("publication_time"));
                    State state = state(rs);
                    int retries = rs.getInt("retries");
                    String errorMessage = rs.getString("error_message");
                    String versionId = rs.getString("version_id");
                    Object serializedMessage = serializedMessage(rs);

                    callback.found(
                            MessageProcessing.create(
                                    new MessageDetails(queueId, messageId, publicationTime),
                                    consumerById.get(consumerId),
                                    new MessageProcessingStatus(state, retries, errorMessage, now(), versionId)
                            ), serializedMessage);
                }
                rs.close();
                ps.close();
                return null;
            }
        });
    }

    public Map<MessageConsumer<?>, Integer> messageCountByConsumer(final Collection<MessageConsumer<?>> consumers, final MessageProcessingFilter filter) {
        return withConnection(new ConnectionCallback<Map<MessageConsumer<?>, Integer>>() {
            public Map<MessageConsumer<?>, Integer> execute(Connection connection) throws SQLException {
                Map<MessageConsumer<?>, Integer> countByConsumer = new HashMap<MessageConsumer<?>, Integer>();
                for (MessageConsumer<?> consumer : consumers)
                    countByConsumer.put(consumer, 0);
                Map<String, MessageConsumer<?>> consumerById = consumersById(consumers);
                ConstraintBuilder constraintBuilder = new ConstraintBuilder(consumers, filter, clock);
                PreparedStatement ps = connection.prepareStatement("" +
                        "SELECT consumer_id, count(*) message_count\n" +
                        "FROM " + tablePrefix + "message_processing mc\n" +
                        "JOIN " + tablePrefix + "message m ON mc.message_id = m.id\n" +
                        "WHERE " + constraintBuilder.whereClause() + "\n" +
                        "GROUP BY consumer_id");
                constraintBuilder.bind(ps);
                ResultSet rs = ps.executeQuery();
                while (rs.next())
                    countByConsumer.put(consumerById.get(rs.getString("consumer_id")), rs.getInt("message_count"));
                rs.close();
                ps.close();
                return countByConsumer;
            }
        });
    }

    private Date toDate(Timestamp timestamp) {
        return new Date(timestamp.getTime());
    }

    private Date now() {
        return new Date(clock.millis());
    }

    private Map<String, MessageConsumer<?>> consumersById(Collection<MessageConsumer<?>> consumers) {
        Map<String, MessageConsumer<?>> consumerById = new HashMap<String, MessageConsumer<?>>();
        for (MessageConsumer<?> consumer : consumers)
            consumerById.put(consumer.id, consumer);
        return consumerById;
    }

    private State state(ResultSet rs) throws SQLException {
        long timesOut = rs.getTimestamp("times_out").getTime();
        return timesOut < clock.millis() ? TIMED_OUT : valueOf(rs.getString("state"));
    }

    private Object serializedMessage(ResultSet rs) throws SQLException {
        String stringMessage = rs.getString("message_string");
        byte[] bytesMessage = rs.getBytes("message_bytes");
        return stringMessage == null ? bytesMessage : stringMessage;
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