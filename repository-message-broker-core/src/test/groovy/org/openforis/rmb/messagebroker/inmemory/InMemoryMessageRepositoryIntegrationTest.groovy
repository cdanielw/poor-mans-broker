package org.openforis.rmb.messagebroker.inmemory

import org.openforis.rmb.messagebroker.AbstractMessageRepositoryIntegrationTest

class InMemoryMessageRepositoryIntegrationTest extends AbstractMessageRepositoryIntegrationTest {
    InMemoryMessageRepository repository = new InMemoryMessageRepository()

    void withTransaction(Closure unitOfWork) {
        unitOfWork()
    }

    def setup() {
        repository.clock = clock
    }
}
