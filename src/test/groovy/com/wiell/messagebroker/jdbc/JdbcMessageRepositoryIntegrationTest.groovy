package com.wiell.messagebroker.jdbc

import com.wiell.messagebroker.AbstractMessageRepositoryIntegrationTest
import integration.TestConnectionManager
import util.Database

class JdbcMessageRepositoryIntegrationTest extends AbstractMessageRepositoryIntegrationTest {
    def database = new Database()
    def connectionManager = new TestConnectionManager(database.dataSource)
    JdbcMessageRepository repository = new JdbcMessageRepository(connectionManager)

    def setup() {
        repository.clock = clock
    }

    void inTransaction(Closure unitOfWork) {
        connectionManager.inTransaction(unitOfWork)
    }

}

