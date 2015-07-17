package com.wiell.messagebroker.jdbc

import com.wiell.messagebroker.AbstractMessageRepositoryIntegrationTest
import integration.TestConnectionManager
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
        callback.clear()

        when:
            take((consumer): 1)

        then:
            callback.gotNoMessages()

    }

}

