package org.openforis.rmb.messagebroker.jdbc;

import org.openforis.rmb.messagebroker.spi.Clock;
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.COMPLETED;

final class MessageProcessingUpdater extends Operation {
    MessageProcessingUpdater(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    Boolean update(MessageProcessingUpdate update) throws SQLException {
        return update.toState == COMPLETED
                ? deleteCompleted(update)
                : updateMessageProcessing(update);
    }

    private boolean deleteCompleted(MessageProcessingUpdate update) throws SQLException {
        boolean success = deleteFromMessageConsumer(update);
        if (success)
            deleteOrphanedMessages();
        connection.commit();
        return success;
    }

    private boolean deleteFromMessageConsumer(MessageProcessingUpdate update) throws SQLException {
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
}
