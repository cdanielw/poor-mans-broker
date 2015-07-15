package com.wiell.messagebroker

import integration.QueueTestDelegate
import spock.lang.Specification
import util.TestHandler

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.*

class WorkerTest extends Specification {
    @SuppressWarnings("GroovyUnusedDeclaration")
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()
    def repo = new MockRepo()
    def throttler = Mock(Throttler)
    def messageId = 'message-id'
    def message = 'message'

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

    def 'When handling is retried, throttler is invoked'() {
        def handler = createFailingHandler(2, 'Error message')
        def consumer = retryingConsumer(3, handler)

        when:
            consume(consumer)

        then:
            1 * throttler.throttle(1, consumer, _ as KeepAlive)
        then:
            1 * throttler.throttle(2, consumer, _ as KeepAlive)
    }

    void consume(MessageConsumer consumer) {
        def update = MessageProcessingUpdate.create(consumer, messageId, PROCESSING, 0, null, null)
        new Worker(repo, throttler, update, message).consume()
    }

    MessageConsumer consumer(TestHandler handler) {
        MessageConsumer.builder('consumer', handler).build()
    }

    MessageConsumer retryingConsumer(int maxRetries, TestHandler handler) {
        MessageConsumer.builder('consumer', handler)
                .retry(maxRetries, ThrottlingStrategy.NO_THROTTLING)
                .build()
    }

    static class MockRepo implements MessageRepository {
        List<MessageProcessingUpdate> updates = []

        void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws MessageRepositoryException {
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
