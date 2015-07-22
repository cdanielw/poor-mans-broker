package org.openforis.rmb.messagebroker

import org.openforis.rmb.messagebroker.spi.MessageTakenCallback
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate
import org.openforis.rmb.messagebroker.spi.MessageRepository
import spock.lang.Specification
import util.AdjustableClock

import static groovyx.gpars.GParsPool.withPool
import static org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate.Status.PENDING
import static org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate.Status.PROCESSING

abstract class AbstractMessageRepositoryIntegrationTest extends Specification {
    def callback = new MockTakenCallback()
    def clock = new AdjustableClock()

    abstract MessageRepository getRepository()

    abstract void withTransaction(Closure unitOfWork)

    def 'When taking message, callback is invoked with the message and an update from PENDING to PROCESSING'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)

        when:
            take((consumer): 1)

        then:
            def callbackInvocation = callback.gotOneMessage('A message', consumer)
            callbackInvocation.update.fromStatus == PENDING
            callbackInvocation.update.toStatus == PROCESSING
    }

    def 'Given no message, when taking message, callback is not invoked'() {
        def consumer = consumer('consumer id')

        when:
            take((consumer): 1)

        then:
            callback.gotNoMessages()
    }

    def 'Given two messages, when taking one message, callback is only invoked for the first message'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)

        when:
            take((consumer): 1)

        then:
            callback.gotOneMessage('message 1')
    }

    def 'Given one message, when taking two messages, callback is invoked for the existing message'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)

        when:
            take((consumer): 2)

        then:
            callback.gotOneMessage('message 1')
    }

    def 'Given two messages, when taking two messages, callback is invoked for both in order'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)

        when:
            take((consumer): 2)

        then:
            callback.messages.size() == 2
            callback[0].message == 'message 1'
            callback[1].message == 'message 2'
    }

    def 'Given two consumers, when taking message for first consumer, callback is only invoked for that consumer'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        addMessage('message 1', consumer1)
        addMessage('message 2', consumer2)

        when:
            take((consumer1): 2)

        then:
            callback.gotOneMessage('message 1', consumer1)
    }

    def 'Given a taken message, when taking messages, callback is not invoked'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)
        takeWithoutCallback((consumer): 1)

        when:
            take((consumer): 1)

        then:
            callback.gotNoMessages()
    }

    def 'Given a timed out message, when taking messages, callback is invoked with the message and an update from PROCESSING to PROCESSING'() {
        def consumer = consumer('consumer id')

        clock.inThePast(consumer.timeout + 1, consumer.timeUnit) {
            addMessage('A message', consumer)
            takeWithoutCallback((consumer): 1)
        }

        when:
            take((consumer): 1)

        then:
            def callbackInvocation = callback.gotOneMessage('A message')
            callbackInvocation.update.fromStatus == PROCESSING
            callbackInvocation.update.toStatus == PROCESSING
    }

    def 'When concurrently trying to take messages, each message is only taken once'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)
        def takers = (1..10)

        when:
            withPool {
                takers.eachParallel {
                    take((consumer): 1)
                }
            }

        then:
            callback.gotOneMessage('A message')
    }

    def 'Given a byte[] message, when taking the message, the byte[] is returned'() {
        def message = 'a byte array'.bytes
        def consumer = consumer('consumer id')
        addMessage(message, consumer)


        when:
            take((consumer): 1)

        then:
            callback.gotOneMessage(message)
    }


    def 'Given two consumers and no messages, message queue sizes are empty'() {
        consumer('consumer 1')
        consumer('consumer 2')
        expect:
            repository.messageQueueSizeByConsumerId().isEmpty()
    }

    def 'Given two consumers and a message for first, two for second, message queue sizes are 1 and 2'() {
        def consumer1 = consumer('consumer 1')
        addMessage('message', consumer1)

        def consumer2 = consumer('consumer 2')
        addMessage('message', consumer2)
        addMessage('message', consumer2)
        expect:
            repository.messageQueueSizeByConsumerId() == [
                    'consumer 1': 1,
                    'consumer 2': 2
            ]
    }

    def 'Given two messages where one is taken, queue size is 1'() {
        def consumer = consumer('consumer')
        addMessage('A message', consumer)
        addMessage('Another message', consumer)
        take((consumer): 1)

        expect:
            repository.messageQueueSizeByConsumerId() == [
                    'consumer': 1
            ]
    }


    def 'Given two messages where one is timed out, queue size is 2'() {
        def consumer = consumer('consumer')

        clock.inThePast(consumer.timeout + 1, consumer.timeUnit) {
            addMessage('A message', consumer)
            addMessage('Another message', consumer)
            takeWithoutCallback((consumer): 1)
        }

        expect:
            repository.messageQueueSizeByConsumerId() == [
                    'consumer': 2
            ]
    }

    void take(Map<MessageConsumer, Integer> maxCountbyConsumer) {
        repository.take(maxCountbyConsumer, callback)
    }


    void takeWithoutCallback(Map<MessageConsumer, Integer> maxCountbyConsumer) {
        repository.take(maxCountbyConsumer, Mock(MessageTakenCallback))
    }

    MessageConsumer consumer(String id, int workCount = Integer.MAX_VALUE) {
        MessageConsumer.builder(id, {} as MessageHandler)
                .messagesHandledInParallel(workCount)
                .build()
    }


    void addMessage(message, MessageConsumer... consumers) {
        withTransaction {
            repository.add('queue id', consumers as List, message)
        }
    }

    static class MockTakenCallback implements MessageTakenCallback {
        final List<CallbackInvocation> messages = []

        void messageTaken(MessageProcessingUpdate update, Object serializedMessage) {
            messages << new CallbackInvocation(update, serializedMessage)
        }

        CallbackInvocation getAt(int index) {
            messages[index]
        }

        CallbackInvocation gotOneMessage(message, MessageConsumer consumer = null) {
            assert messages.size() == 1
            assert messages[0].message == message
            if (consumer)
                assert messages[0].update.consumer == consumer
            return messages[0]
        }

        void gotNoMessages() {
            assert messages.empty
        }

        void clear() {
            messages.clear()
        }
    }

    static class CallbackInvocation {
        final MessageProcessingUpdate update
        final message

        CallbackInvocation(MessageProcessingUpdate update, Object message) {
            this.update = update
            this.message = message
        }

        String toString() {
            return message
        }
    }

}
