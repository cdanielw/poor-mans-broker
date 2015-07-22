package org.openforis.rmb.messagebroker.jdbc

import groovy.sql.Sql
import integration.TestConnectionManager
import org.openforis.rmb.messagebroker.AbstractMessageRepositoryIntegrationTest
import org.openforis.rmb.messagebroker.spi.MessageDetails
import org.openforis.rmb.messagebroker.spi.MessageProcessingCallback
import org.openforis.rmb.messagebroker.spi.MessageProcessingFilter
import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus
import util.Database

class JdbcMessageRepositoryIntegrationTest extends AbstractMessageRepositoryIntegrationTest {
    def database = new Database()
    def connectionManager = new TestConnectionManager(database.dataSource)
    JdbcMessageRepository repository = new JdbcMessageRepository(connectionManager, '')

    def setup() {
        repository.clock = clock
    }

    void withTransaction(Closure unitOfWork) {
        connectionManager.withTransaction(unitOfWork)
    }

//    def 'Test'() {
//        def filter = new MessageProcessingFilter()
//         callback = new MessageProcessingCallback() {
//            void messageProcessing(MessageDetails messageDetails, MessageProcessingStatus status, Object serializedMessage) {
//
//            }
//        }
//
//        when: repository.findMessageProcessing(filter, callback)
//        then:
//            false
//    }


    def 'A blocking consumer will not let a message be be taken while another is processing'() {
        def consumer = consumer('consumer id', 1)
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)
        take((consumer): 1)
        callback.clear()

        when:
            take((consumer): 1)

        then:
            callback.gotNoMessages()
    }

    def 'When a message is consumed, it is removed from message_consumer and message tables'() {
        def consumer = consumer('consumer id', 1)
        addMessage('A message', consumer)
        take((consumer): 1)
        def update = callback.messages.first().update

        when:
            repository.update(update.completed())

        then:
            sql.firstRow('SELECT count(*) c FROM message_consumer').c == 0
            sql.firstRow('SELECT count(*) c FROM message').c == 0
    }

    def 'Given a message with multiple consumer, when a message is consumed by one consumer, it is removed from message_consumer but not from message'() {
        def consumer1 = consumer('consumer 1', 1)
        def consumer2 = consumer('consumer 2', 1)
        addMessage('A message', consumer1, consumer2)
        take((consumer1): 1)

        def update = callback.messages.first().update

        when:
            repository.update(update.completed())

        then:
            sql.firstRow('SELECT count(*) c FROM message_consumer').c == 1 // consumer 2 row is left
            sql.firstRow('SELECT count(*) c FROM message').c == 1
    }


    private Sql getSql() {
        new Sql(database.dataSource)
    }
}

