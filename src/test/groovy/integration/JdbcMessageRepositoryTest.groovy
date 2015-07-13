package integration

import com.wiell.messagebroker.MessageConsumer
import com.wiell.messagebroker.MessageHandler
import com.wiell.messagebroker.jdbc.JdbcConnectionManager
import com.wiell.messagebroker.jdbc.JdbcMessageRepository
import com.wiell.messagebroker.spi.MessageCallback
import spock.lang.Specification
import util.Database

import javax.sql.DataSource
import java.sql.Connection
import java.sql.SQLException

class JdbcMessageRepositoryTest extends Specification {
    def database = new Database()
    def connectionManager = new ManagedConnectionConnectionManager(database.dataSource)
    def callback = Mock(MessageCallback)
    def repository = new JdbcMessageRepository(connectionManager)

    def 'When taking message, callback is invoked'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)

        when:
            repository.takeMessages([(consumer): 1], callback)

        then:
            1 * callback.messageTaken(consumer, _ as String, 'A message')
    }

    def 'Given no message, when taking message, callback is not invoked'() {
        def consumer = consumer('consumer id')

        when:
            repository.takeMessages([(consumer): 1], callback)

        then:
            0 * callback.messageTaken(*_)
    }

    def 'Given two messages, when taking one message, callback is only invoked for the first message'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)

        when:
            repository.takeMessages([(consumer): 1], callback)

        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 1')
            0 * callback.messageTaken(*_)
    }

    def 'Given one message, when taking two messages, callback is invoked for the existing message'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)

        when:
            repository.takeMessages([(consumer): 2], callback)

        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 1')
            0 * callback.messageTaken(*_)
    }

    def 'Given two messages, when taking two messages, callback is invoked for both in order'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)

        when:
            repository.takeMessages([(consumer): 2], callback)

        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 1')
        then:
            1 * callback.messageTaken(consumer, _ as String, 'message 2')
            0 * callback.messageTaken(*_)
    }

    def 'Given two consumers, when taking message for first consumer, callback is only invoked for that consumer'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        addMessage('message 1', consumer1)
        addMessage('message 2', consumer2)

        when:
            repository.takeMessages([(consumer1): 2], callback)

        then:
            1 * callback.messageTaken(consumer1, _ as String, 'message 1')
            0 * callback.messageTaken(*_)
    }

    MessageConsumer consumer(String id) {
        MessageConsumer.builder(id, {} as MessageHandler).build()
    }

    void addMessage(String message, MessageConsumer... consumers) {
        connectionManager.inTransaction {
            repository.addMessage('queue id', consumers as List, message)
        }
    }
}

class ManagedConnectionConnectionManager implements JdbcConnectionManager {
    private final DataSource dataSource
    private Connection connection
    private boolean inTransaction

    ManagedConnectionConnectionManager(DataSource dataSource) {
        this.dataSource = dataSource
    }

    Connection getConnection() {
        if (!connection)
            connection = dataSource.connection
        return connection
    }

    void releaseConnection(Connection connection) throws SQLException {
        if (!inTransaction) {
            connection.close()
            connection = null
        }
    }

    void inTransaction(Closure unitOfWork) {
        inTransaction = true
        try {
            connection = dataSource.connection
            unitOfWork()
            connection.commit()
        } finally {
            connection?.close()
            connection = null
            inTransaction = false
        }
    }
}
