package org.openforis.rmb.messagebroker

import integration.QueueTestDelegate
import org.openforis.rmb.messagebroker.monitor.*
import org.openforis.rmb.messagebroker.spi.*
import spock.lang.Specification
import util.CollectingMonitor
import util.TestHandler

import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*


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
                toState == COMPLETED
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
                toState == PROCESSING
                retries == 1
                errorMessage == 'Error message'
            }
            with(repo[1]) {
                toState == COMPLETED
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
                toState == FAILED
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
                toState == PROCESSING
                retries == 0
            }
            with(repo[1]) {
                toState == COMPLETED
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
        MessageProcessingUpdate update = takeUpdate(consumer, PENDING)
        new Worker(repo, throttler, new Monitors([monitor]), update, message).consume()
    }

    void consumeTimedOut(MessageConsumer consumer) {
        def update = takeUpdate(consumer, TIMED_OUT)
        new Worker(repo, throttler, new Monitors([monitor]), update, message).consume()
    }

    MessageProcessingUpdate takeUpdate(MessageConsumer consumer, MessageProcessingStatus.State fromState) {
        return MessageProcessing.create(new MessageDetails('queue id', messageId, new Date(0)),
                consumer,
                new MessageProcessingStatus(fromState, 0, null, new Date(0), randomUuid())).take(new DefaultThrottlerTest.StaticClock())
    }

    String randomUuid() {
        return UUID.randomUUID().toString();
    }

    MessageConsumer consumer(TestHandler handler) {
        MessageConsumer.builder('consumer', handler).build()
    }

    MessageConsumer retryingConsumer(int maxRetries, TestHandler handler) {
        MessageConsumer.builder('consumer', handler)
                .retry(maxRetries, ThrottlingStrategy.NO_THROTTLING)
                .build()
    }

    private static class MockRepo implements MessageRepository {
        List<MessageProcessingUpdate> updates = []

        void add(String queueId, List<MessageConsumer<?>> consumers, Object serializedMessage) throws MessageRepositoryException {
            assert false, "No call to MessageRepository.add expected"
        }

        void take(Map<MessageConsumer<?>, Integer> maxCountByConsumer, MessageRepository.MessageTakenCallback callback) throws MessageRepositoryException {
            assert false, "No call to MessageRepository.take expected"
        }

        boolean update(MessageProcessingUpdate update) throws MessageRepositoryException {
            updates << update
            versionIdsChainTogether()
            return true
        }

        void findMessageProcessing(Collection<MessageConsumer<?>> consumers,
                                   MessageProcessingFilter filter,
                                   MessageRepository.MessageProcessingFoundCallback callback) {

        }

        Map<MessageConsumer<?>, Integer> messageCountByConsumer(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter) {
            return null
        }

        void deleteMessageProcessing(Collection<MessageConsumer<?>> consumers, MessageProcessingFilter filter) throws MessageRepositoryException {

        }

        void versionIdsChainTogether() {
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
