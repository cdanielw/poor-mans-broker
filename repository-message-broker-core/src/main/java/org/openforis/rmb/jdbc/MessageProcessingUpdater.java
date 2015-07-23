package org.openforis.rmb.jdbc;

import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingUpdate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.openforis.rmb.spi.MessageProcessingStatus.State.COMPLETED;

final class MessageProcessingUpdater extends Operation {
    MessageProcessingUpdater(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    Boolean update(MessageProcessingUpdate update) throws SQLException {
        return update.getToState() == COMPLETED
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
        ps.setString(1, update.getMessageId());
        ps.setString(2, update.getConsumer().getId());
        ps.setString(3, update.getFromVersionId());
        int rowsDeleted = ps.executeUpdate();
        if (rowsDeleted > 1)
            throw new IllegalStateException("More than one row with message_id " + update.getMessageId());
        return rowsDeleted != 0;
    }
}
