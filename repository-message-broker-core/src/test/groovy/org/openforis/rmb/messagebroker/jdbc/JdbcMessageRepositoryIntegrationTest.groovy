package org.openforis.rmb.messagebroker.jdbc

import groovy.sql.Sql
import integration.TestConnectionManager
import org.openforis.rmb.messagebroker.AbstractMessageRepositoryIntegrationTest
import org.openforis.rmb.messagebroker.MessageConsumer
import org.openforis.rmb.messagebroker.spi.*
import spock.lang.Ignore
import util.Database

class JdbcMessageRepositoryIntegrationTest extends AbstractMessageRepositoryIntegrationTest {
    def processingCallback = new MockProcessingCallbackFound()
    def database = new Database()
    def connectionManager = new TestConnectionManager(database.dataSource)
    JdbcMessageRepository repository = new JdbcMessageRepository(connectionManager, '')

    def setup() {
        repository.clock = clock
    }

    void withTransaction(Closure unitOfWork) {
        connectionManager.withTransaction(unitOfWork)
    }

    @Ignore
    def 'Given a message and an empty filter, when finding message processing, callback is invoked'() {
        def consumer = consumer('consumer id')
        addMessage('A message', consumer)

        def filter = MessageProcessingFilter.builder().build();

        when:
            repository.findMessageProcessing(filter, processingCallback)
        then:
            processingCallback.gotOneMessage('A message')
    }

    def 'A blocking consumer will not let a message be be taken while another is processing'() {
        def consumer = consumer('consumer id', 1)
        addMessage('message 1', consumer)
        addMessage('message 2', consumer)
        take((consumer): 1)
        takenCallback.reset()

        when:
            take((consumer): 1)

        then:
            takenCallback.notInvoked()
    }

    def 'When a message is consumed, it is removed from message_consumer and message tables'() {
        def consumer = consumer('consumer id', 1)
        addMessage('A message', consumer)
        take((consumer): 1)
        def update = takenCallback.invocations.first().update

        when:
            repository.update(update.completed())

        then:
            sql.firstRow('SELECT count(*) c FROM message_consumer').c == 0
            sql.firstRow('SELECT count(*) c FROM message').c == 0
    }

    def 'Given a message with multiple consumer, when a message is consumed by one consumer, it is removed from message_consumer but not from message'() {
        def consumer1 = consumer('consumer 1', 1)
        def consumer2 = consumer('consumer 2', 1)
        addMessage('A message', consumer1, consumer2)
        take((consumer1): 1)

        def update = takenCallback.invocations.first().update

        when:
            repository.update(update.completed())

        then:
            sql.firstRow('SELECT count(*) c FROM message_consumer').c == 1 // consumer 2 row is left
            sql.firstRow('SELECT count(*) c FROM message').c == 1
    }


    private Sql getSql() {
        new Sql(database.dataSource)
    }

    static class MockProcessingCallbackFound implements MessageRepository.MessageProcessingFoundCallback {
        final List<ProcessingCallbackInvocation> invocations = []

        void found(String consumerId, MessageDetails messageDetails, MessageProcessingStatus status, Object serializedMessage) {
            invocations << new ProcessingCallbackInvocation(consumerId, messageDetails, status, serializedMessage)
        }


        ProcessingCallbackInvocation getAt(int index) {
            invocations[index]
        }

        ProcessingCallbackInvocation gotOneMessage(message, MessageConsumer consumer = null) {
            assert invocations.size() == 1
            assert invocations[0].message == message
            if (consumer)
                assert invocations[0].consumerId == consumer
            return invocations[0]
        }

        void notInvoked() {
            assert invocations.empty
        }

        void reset() {
            invocations.clear()
        }
    }

    static class ProcessingCallbackInvocation {
        final String consumerId
        final MessageDetails messageDetails
        final MessageProcessingStatus status
        final Object message

        ProcessingCallbackInvocation(String consumerId, MessageDetails messageDetails, MessageProcessingStatus status, Object message) {
            this.consumerId = consumerId
            this.messageDetails = messageDetails
            this.status = status
            this.message = message
        }

        String toString() {
            return message
        }
    }
}

