package com.wiell.messagebroker

import integration.QueueTestDelegate
import org.spockframework.lang.ConditionBlock
import spock.lang.Specification
import util.TestHandler

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.*

class WorkerTest extends Specification {
    @SuppressWarnings("GroovyUnusedDeclaration")
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()
    def repo = new MockRepo()
    private String messageId = 'message-id'
    private String message = 'message'

    def 'Passes message to consumer and updates the repository'() {
        def handler = createHandler()

        when:
            consume(consumer(handler))

        then:
            handler.handled(message)
            repo.updates.size() == 1
            with(repo[0]) {
                status == COMPLETED
                retries == 0
            }
    }

    def 'Given a retrying consumer, when handling fails, it is retried, and repository is updated'() {
        def handler = createFailingHandler(1, 'Error message')

        when:
            consume(retryingConsumer(1, handler))

        then:
            handler.handled(message)
            repo.updates.size() == 2
            with(repo[0]) {
                status == PROCESSING
                retries == 1
                errorMessage == 'Error message'
            }
            with(repo[1]) {
                status == COMPLETED
                retries == 1
                errorMessage == 'Error message'
            }
    }

    def 'Given a non-retrying consumer, when handling fails, repository is notified about the failure'() {
        def handler = createFailingHandler(1)

        when:
            consume(consumer(handler))

        then:
            repo.updates.size() == 1
            with(repo[0]) {
                status == FAILED
                retries == 0
            }
    }

    @ConditionBlock
    void assertEquals(MessageProcessingUpdate update, @DelegatesTo(MessageProcessingUpdate) Closure assertions) {
        assertions.delegate = update
        assertions()
    }

    def 'Given a keep alive handler, when handler calls keep alive, repository is notified'() {
        def consumer = consumer(
                createHandler { message, KeepAlive keepAlive ->
                    keepAlive.send()
                }
        )

        when:
            consume(consumer)

        then:
            repo.updates.size() == 2
            with(repo[0]) {
                status == PROCESSING
                retries == 0
            }
            with(repo[1]) {
                status == COMPLETED
                retries == 0
            }
    }

    void consume(MessageConsumer consumer) {
        new Worker(repo, MessageProcessingUpdate.create(consumer, messageId, PROCESSING, 0, null, null), message).consume()
    }

    private MessageConsumer consumer(TestHandler handler) {
        MessageConsumer.builder('consumer', handler).build()
    }

    private MessageConsumer retryingConsumer(int maxRetries, TestHandler handler) {
        MessageConsumer.builder('consumer', handler)
                .retry(maxRetries, ThrottlingStrategy.NO_THROTTLING)
                .build()
    }

    static class MockRepo implements MessageRepository {
        List<MessageProcessingUpdate> updates = []

        void add(String queueId, List<MessageConsumer<?>> consumers, String serializedMessage) throws MessageRepositoryException {
            assert false, "No call to MessageRepository.add expected"
        }

        void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageCallback callback) throws MessageRepositoryException {
            assert false, "No call to MessageRepository.take expected"
        }

        boolean update(MessageProcessingUpdate update) throws MessageRepositoryException {
            updates << update
            // TODO: Verify version chain - toVersionshould equal fromVersion in next update
        }

        MessageProcessingUpdate getAt(int index) {
            updates[index]
        }
    }
}
