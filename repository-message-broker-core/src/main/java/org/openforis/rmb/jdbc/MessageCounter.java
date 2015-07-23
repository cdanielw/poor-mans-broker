package org.openforis.rmb.jdbc;

import org.openforis.rmb.MessageConsumer;
import org.openforis.rmb.spi.Clock;
import org.openforis.rmb.spi.MessageProcessingFilter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

final class MessageCounter extends Operation {
    public MessageCounter(Connection connection, String tablePrefix, Clock clock) {
        super(connection, tablePrefix, clock);
    }

    Map<MessageConsumer<?>, Integer> countByConsumer(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter) throws SQLException {
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
}
