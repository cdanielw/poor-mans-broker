package org.openforis.rmb.jdbc

import groovy.sql.Sql
import org.openforis.rmb.AbstractMessageRepositoryIntegrationTest
import org.openforis.rmb.TestConnectionManager
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

    def 'A blocking consumer will not let a message be be taken while another is processing'() {
        def consumer = consumer('consumer id', 1)
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)
        take((consumer): 1)
        takenCallback.reset()

        when:
            take((consumer): 1)

        then:
            takenCallback.notInvoked()
    }

    def 'When a message is consumed, it is removed from message_processing and message tables'() {
        def consumer = consumer('consumer id', 1)
        addMessage('A message', consumer)
        take((consumer): 1)
        def update = takenCallback.invocations.first().update

        when:
            repository.update(update.completed(clock))

        then:
            sql.firstRow('SELECT count(*) c FROM message_processing').c == 0
            sql.firstRow('SELECT count(*) c FROM message').c == 0
    }

    def 'Given a message with multiple consumer, when a message is consumed by one consumer, it is removed from message_processing but not from message'() {
        def consumer1 = consumer('consumer 1', 1)
        def consumer2 = consumer('consumer 2', 1)
        addMessage('A message', consumer1, consumer2)
        take((consumer1): 1)

        def update = takenCallback.invocations.first().update

        when:
            repository.update(update.completed(clock))

        then:
            sql.firstRow('SELECT count(*) c FROM message_processing').c == 1 // consumer 2 row is left
            sql.firstRow('SELECT count(*) c FROM message').c == 1
    }


    private Sql getSql() {
        new Sql(database.dataSource)
    }
}

