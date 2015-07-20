package com.wiell.messagebroker

import com.wiell.messagebroker.monitor.*
import integration.QueueTestDelegate
import spock.lang.Specification
import util.CollectingMonitor
import util.TestHandler

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.*

class WorkerTest extends Specification {
    @SuppressWarnings("GroovyUnusedDeclaration")
    @Delegate QueueTestDelegate queueTestDelegate = new QueueTestDelegate()
    def repo = new MockRepo()
    def monitor = new CollectingMonitor()
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
                toStatus == COMPLETED
                retries == 0
            }
    }

    def 'Given a retrying consumer, when handling fails, it is retried, repository is updated, and monitors get a RetryingMessageConsumptionEvent'() {
        def handler = createFailingHandler(1, 'Error message')

        when:
            consume(retryingConsumer(1, handler))

        then:
            handler.handled(message)
            repo.updates.size() == 2
            with(repo[0]) {
                toStatus == PROCESSING
                retries == 1
                errorMessage == 'Error message'
            }
            with(repo[1]) {
                toStatus == COMPLETED
                retries == 1
                errorMessage == 'Error message'
            }
            monitor.eventTypes().contains(RetryingMessageConsumptionEvent)
    }

    def 'Given a non-retrying consumer, when handling fails, repository is notified about the failure'() {
        def handler = createFailingHandler(1)

        when:
            consume(consumer(handler))

        then:
            repo.updates.size() == 1
            with(repo[0]) {
                toStatus == FAILED
                retries == 0
            }
    }

    def 'Given a keep alive handler, when handler calls keep alive, repository is notified and monitor is notified about a MessageKeptAliveEvent'() {
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
                toStatus == PROCESSING
                retries == 0
            }
            with(repo[1]) {
                toStatus == COMPLETED
                retries == 0
            }
            monitor.eventTypes().contains(MessageKeptAliveEvent)
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

    def 'When handling a message, monitors are passed a ConsumingNewMessageEvent and MessageConsumedEvent '() {
        def handler = createHandler()

        when:
            consume(consumer(handler))
        then:
            monitor.eventTypes() == [ConsumingNewMessageEvent, MessageConsumedEvent]
    }

    def 'When handling a timed out message, monitors are passed a ConsumingTimedOutMessageEvent and MessageConsumedEvent '() {
        def handler = createHandler()

        when:
            consumeTimedOut(consumer(handler))
        then:
            monitor.eventTypes() == [ConsumingTimedOutMessageEvent, MessageConsumedEvent]
    }

    def 'When retrying, monitors are passed a ThrottlingMessageRetryEvent'() {
        def consumer = retryingConsumer(1, createFailingHandler(1, 'Error message'))

        when:
            consume(consumer)
        then:
            monitor.eventTypes() == [
                    ConsumingNewMessageEvent,
                    ThrottlingMessageRetryEvent,
                    RetryingMessageConsumptionEvent,
                    MessageConsumedEvent]
    }


    void consume(MessageConsumer consumer) {
        def update = MessageProcessingUpdate.create('queue id', consumer, messageId, 0, PENDING, PROCESSING, 0, null, UUID.randomUUID().toString())
        new Worker(repo, throttler, new Monitors([monitor]), update, message).consume()
    }

    void consumeTimedOut(MessageConsumer consumer) {
        def update = MessageProcessingUpdate.create('queue id', consumer, messageId, 0, PROCESSING, PROCESSING, 0, null, UUID.randomUUID().toString())
        new Worker(repo, throttler, new Monitors([monitor]), update, message).consume()
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
            versionIdsChainTogether()
            return true
        }

        Map<String, Integer> messageQueueSizeByConsumerId() {
            return Collections.EMPTY_MAP;
        }

        private void versionIdsChainTogether() {
            if (updates) {
                updates.tail().inject(updates.first()) { prevUpdate, update ->
                    def toVersionId = prevUpdate.toVersionId
                    def fromVersionId = update.fromVersionId
                    assert toVersionId == fromVersionId
                    return update
                }
            }
        }

        MessageProcessingUpdate getAt(int index) {
            updates[index]
        }
    }
}
