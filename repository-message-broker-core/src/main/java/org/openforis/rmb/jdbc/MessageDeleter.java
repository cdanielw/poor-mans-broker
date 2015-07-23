package org.openforis.rmb.jdbc;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingFilter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

final class MessageDeleter extends Operation {
    public MessageDeleter(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    void delete(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter) throws SQLException {
        ConstraintBuilder constraintBuilder = new ConstraintBuilder(consumers, filter, clock);
        PreparedStatement ps = connection.prepareStatement("" +
                "DELETE FROM " + tablePrefix + "message_processing WHERE " + constraintBuilder.whereClause());
        constraintBuilder.bind(ps);
        int rowsDeleted = ps.executeUpdate();
        if (rowsDeleted > 0) {
            deleteOrphanedMessages();
            connection.commit();
        }
    }
}
