package org.openforis.rmb.inmemory

import org.openforis.rmb.AbstractMessageRepositoryIntegrationTest

class InMemoryMessageRepositoryIntegrationTest extends AbstractMessageRepositoryIntegrationTest {
    InMemoryMessageRepository repository = new InMemoryMessageRepository()

    void withTransaction(Closure unitOfWork) {
        unitOfWork()
    }

    def setup() {
        repository.clock = clock
    }
}
