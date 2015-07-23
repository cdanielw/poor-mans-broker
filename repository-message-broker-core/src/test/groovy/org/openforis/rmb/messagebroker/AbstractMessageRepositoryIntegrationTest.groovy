package org.openforis.rmb.messagebroker

import org.openforis.rmb.messagebroker.spi.MessageProcessing
import org.openforis.rmb.messagebroker.spi.MessageProcessingFilter
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate
import org.openforis.rmb.messagebroker.spi.MessageRepository
import spock.lang.Specification
import util.AdjustableClock

import static groovyx.gpars.GParsPool.withPool
import static org.openforis.rmb.messagebroker.spi.MessageProcessingStatus.State.*

abstract class AbstractMessageRepositoryIntegrationTest extends Specification {
    def takenCallback = new MockTakenCallback()
    def processingCallback = new MockProcessingFoundCallback()
    def clock = new AdjustableClock()

    abstract MessageRepository getRepository()

    abstract void withTransaction(Closure unitOfWork)

    def 'When taking message, callback is invoked with the message and an update from PENDING to PROCESSING'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)

        when:
            take((consumer): 1)

        then:
            def callbackInvocation = takenCallback.gotOneMessage('A message', consumer)
            callbackInvocation.update.fromState == PENDING
            callbackInvocation.update.toState == PROCESSING
    }

    def 'Given no message, when taking message, callback is not invoked'() {
        def consumer = consumer('consumer id')

        when:
            take((consumer): 1)

        then:
            takenCallback.notInvoked()
    }

    def 'Given two messages, when taking one message, callback is only invoked for the first message'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)

        when:
            take((consumer): 1)

        then:
            takenCallback.gotOneMessage('message 1')
    }

    def 'Given one message, when taking two messages, callback is invoked for the existing message'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)

        when:
            take((consumer): 2)

        then:
            takenCallback.gotOneMessage('message 1')
    }

    def 'Given two messages, when taking two messages, callback is invoked for both in order'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)

        when:
            take((consumer): 2)

        then:
            takenCallback.invocations.size() == 2
            takenCallback[0].message == 'message 1'
            takenCallback[1].message == 'message 2'
    }

    def 'Given two consumers, when taking message for first consumer, callback is only invoked for that consumer'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        addMessage('message 1', consumer1)
        addMessage('message 2', consumer2)

        when:
            take((consumer1): 2)

        then:
            takenCallback.gotOneMessage('message 1', consumer1)
    }

    def 'Given a taken message, when taking messages, callback is not invoked'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)
        takeWithoutCallback((consumer): 1)

        when:
            take((consumer): 1)

        then:
            takenCallback.notInvoked()
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
            def callbackInvocation = takenCallback.gotOneMessage('A message')
            callbackInvocation.update.fromState == TIMED_OUT
            callbackInvocation.update.toState == PROCESSING
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
            takenCallback.gotOneMessage('A message')
    }

    def 'Given a byte[] message, when taking the message, the byte[] is returned'() {
        def message = 'a byte array'.bytes
        def consumer = consumer('consumer id')
        addMessage(message, consumer)


        when:
            take((consumer): 1)

        then:
            takenCallback.gotOneMessage(message)
    }


    def 'Given two consumers and no messages, message counts are 0 for both'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        expect:
            repository.messageCountByConsumer(
                    [consumer1, consumer2],
                    MessageProcessingFilter.builder().build()
            ) == [(consumer1): 0, (consumer2): 0]
    }

    def 'Given two consumers and a message for first, two for second, message count are 1 and 2'() {
        def consumer1 = consumer('consumer 1')
        addMessage('message', consumer1)

        def consumer2 = consumer('consumer 2')
        addMessage('message', consumer2)
        addMessage('message', consumer2)
        expect:
            repository.messageCountByConsumer(
                    [consumer1, consumer2],
                    MessageProcessingFilter.builder().build()
            ) == [(consumer1): 1, (consumer2): 2]
    }

    def 'Given two messages where one is taken, PENDING message count is 1'() {
        def consumer = consumer('consumer')
        addMessage('A message', consumer)
        addMessage('Another message', consumer)
        take((consumer): 1)

        expect:
            repository.messageCountByConsumer([consumer],
                    MessageProcessingFilter.builder()
                            .states(PENDING)
                            .build()
            ) == [(consumer): 1]
    }


    def 'Given two messages where one is timed out, PENDING or TIMED_OUT message count is 2'() {
        def consumer = consumer('consumer')

        clock.inThePast(consumer.timeout + 1, consumer.timeUnit) {
            addMessage('A message', consumer)
            addMessage('Another message', consumer)
            takeWithoutCallback((consumer): 1)
        }

        expect:
            repository.messageCountByConsumer([consumer],
                    MessageProcessingFilter.builder()
                            .states(PENDING, TIMED_OUT)
                            .build()
            ) == [(consumer): 2]
    }

    def 'Finding messages with empty filters'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)

        def filter = MessageProcessingFilter.builder().build();

        when:
            repository.findMessageProcessing([consumer], filter, processingCallback)
        then:
            processingCallback.invokedOnce('A message')
    }

    def 'Finding messages filters on consumers'() {
        def consumer1 = consumer('consumer 1')
        def consumer2 = consumer('consumer 2')
        addMessage('message 1', consumer1)
        addMessage('message 2', consumer2)

        def filter = MessageProcessingFilter.builder().build();

        when:
            repository.findMessageProcessing([consumer1], filter, processingCallback)
        then:
            def invocation = processingCallback.invokedOnce('message 1')
            invocation.messageProcessing.consumer == consumer1
    }

    def 'Finding messages filters on state'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)
        takeWithoutCallback((consumer): 1)

        def filter = MessageProcessingFilter.builder()
                .states(PROCESSING)
                .build();

        when:
            repository.findMessageProcessing([consumer], filter, processingCallback)
        then:
            processingCallback.invokedOnce('message 1')
    }

    def 'Finding messages finds TIMED_OUT'() {
        def consumer = consumer('consumer id')

        clock.inThePast(consumer.timeout + 1, consumer.timeUnit) {
            addMessage('A message', consumer)
            takeWithoutCallback((consumer): 1)
        }

        def filter = MessageProcessingFilter.builder()
                .states(TIMED_OUT)
                .build();

        when:
            repository.findMessageProcessing([consumer], filter, processingCallback)
        then:
            processingCallback.invokedOnce('A message')
    }

    def 'Finding messages filters on date published'() {
        def consumer = consumer('consumer id')

        def clock = new DefaultThrottlerTest.StaticClock()
        repository.clock = clock
        clock.time = 99
        addMessage('Message published before', consumer)
        clock.time = 100
        addMessage('Message published on', consumer)
        clock.time = 101
        addMessage('Message published after', consumer)


        when:
            repository.findMessageProcessing([consumer],
                    MessageProcessingFilter.builder()
                            .publishedBefore(new Date(100))
                            .build(),
                    processingCallback)
        then:
            processingCallback.invokedOnce('Message published before')

            processingCallback.reset()

        when:
            repository.findMessageProcessing([consumer],
                    MessageProcessingFilter.builder()
                            .publishedAfter(new Date(100))
                            .build(),
                    processingCallback)
        then:
            processingCallback.invokedOnce('Message published after')
    }

    def 'Finding messages filters on last updated'() {
        def consumer = consumer('consumer id')

        def clock = new DefaultThrottlerTest.StaticClock()
        repository.clock = clock

        addMessage('Message updated before', consumer)
        addMessage('Message updated on', consumer)
        addMessage('Message updated after', consumer)

        clock.time = 99
        take((consumer): 1)
        clock.time = 100
        take((consumer): 2)
        clock.time = 101
        take((consumer): 3)

        when:
            repository.findMessageProcessing([consumer],
                    MessageProcessingFilter.builder()
                            .lastUpdatedBefore(new Date(100))
                            .build(),
                    processingCallback)
        then:
            processingCallback.invokedOnce('Message updated before')

            processingCallback.reset()

        when:
            repository.findMessageProcessing([consumer],
                    MessageProcessingFilter.builder()
                            .lastUpdatedAfter(new Date(100))
                            .build(),
                    processingCallback)
        then:
            processingCallback.invokedOnce('Message updated after')
    }

    def 'Finding messages by id'() {
        def consumer = consumer('consumer id')
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)
        take((consumer): 1)
        def messageId = takenCallback.invocations.first().update.messageId

        def filter = MessageProcessingFilter.builder()
                .messageIds(messageId)
                .build();

        when:
            repository.findMessageProcessing([consumer], filter, processingCallback)
        then:
            processingCallback.invokedOnce('message 1')
    }

    def 'Deletes messages'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)

        when:
            repository.deleteMessageProcessing([consumer], MessageProcessingFilter.builder().build())

        then:
            repository.messageCountByConsumer(
                    [consumer],
                    MessageProcessingFilter.builder().build()
            ) == [(consumer): 0]
    }


    void take(Map<MessageConsumer, Integer> maxCountbyConsumer) {
        repository.take(maxCountbyConsumer, takenCallback)
    }


    void takeWithoutCallback(Map<MessageConsumer, Integer> maxCountbyConsumer) {
        repository.take(maxCountbyConsumer, Mock(MessageRepository.MessageTakenCallback))
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

    static class MockTakenCallback implements MessageRepository.MessageTakenCallback {
        final List<TakenCallbackInvocation> invocations = []

        void taken(MessageProcessingUpdate update, Object serializedMessage) {
            invocations << new TakenCallbackInvocation(update, serializedMessage)
        }

        TakenCallbackInvocation getAt(int index) {
            invocations[index]
        }

        TakenCallbackInvocation gotOneMessage(message, MessageConsumer consumer = null) {
            assert invocations.size() == 1
            assert invocations[0].message == message
            if (consumer)
                assert invocations[0].update.consumer == consumer
            return invocations[0]
        }

        void notInvoked() {
            assert invocations.empty
        }

        void reset() {
            invocations.clear()
        }
    }

    static class TakenCallbackInvocation {
        final MessageProcessingUpdate update
        final message

        TakenCallbackInvocation(MessageProcessingUpdate update, Object message) {
            this.update = update
            this.message = message
        }

        String toString() {
            return message
        }
    }

    static class MockProcessingFoundCallback implements MessageRepository.MessageProcessingFoundCallback {
        final List<ProcessingCallbackInvocation> invocations = []

        void found(MessageProcessing messageProcessing, Object serializedMessage) {
            invocations << new ProcessingCallbackInvocation(messageProcessing, serializedMessage)
        }


        ProcessingCallbackInvocation getAt(int index) {
            invocations[index]
        }

        ProcessingCallbackInvocation invokedOnce(message, MessageConsumer consumer = null) {
            assert invocations.size() == 1
            assert invocations[0].message == message
            if (consumer)
                assert invocations[0].consumerId == consumer
            return invocations[0]
        }

        void reset() {
            invocations.clear()
        }
    }

    static class ProcessingCallbackInvocation {
        final MessageProcessing messageProcessing
        final Object message

        ProcessingCallbackInvocation(MessageProcessing messageProcessing, Object message) {
            this.messageProcessing = messageProcessing
            this.message = message
        }

        String toString() {
            return message
        }
    }

}
