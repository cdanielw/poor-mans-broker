package com.wiell.messagebroker.inmemory

import com.wiell.messagebroker.AbstractMessageRepositoryIntegrationTest

class InMemoryMessageRepositoryIntegrationTest extends AbstractMessageRepositoryIntegrationTest {
    InMemoryMessageRepository repository = new InMemoryMessageRepository()

    void inTransaction(Closure unitOfWork) {
        unitOfWork()
    }

    def setup() {
        repository.clock = clock
    }
}
