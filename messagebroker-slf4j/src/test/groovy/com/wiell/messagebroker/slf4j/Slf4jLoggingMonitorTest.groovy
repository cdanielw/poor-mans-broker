package com.wiell.messagebroker.slf4j

import com.wiell.messagebroker.*
import com.wiell.messagebroker.inmemory.InMemoryMessageRepository
import com.wiell.messagebroker.monitor.*
import spock.lang.Specification
import spock.lang.Unroll
import uk.org.lidalia.slf4jtest.TestLogger
import uk.org.lidalia.slf4jtest.TestLoggerFactory

import static com.wiell.messagebroker.MessageProcessingUpdate.Status.PENDING
import static com.wiell.messagebroker.MessageProcessingUpdate.Status.PROCESSING
import static com.wiell.messagebroker.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER
import static uk.org.lidalia.slf4jext.Level.*

class Slf4jLoggingMonitorTest extends Specification {
    static messageBroker = new PollingMessageBroker(
            MessageBrokerConfig.builder(new InMemoryMessageRepository(), NULL_TRANSACTION_SYNCHRONIZER)
    )
    static consumer = MessageConsumer.builder('consumer id', {} as MessageHandler).build()
    static update = MessageProcessingUpdate.create(consumer, 'message id', PENDING, PROCESSING, 0, null, 'version id')

    def monitor = new Slf4jLoggingMonitor()

    @Unroll
    def '#event.class.simpleName is #level'() {
        TestLogger logger = TestLoggerFactory.getTestLogger(event.class);
        logger.clearAll()

        when:
            monitor.onEvent(event)

        then:
            logger.allLoggingEvents.size() == 1
            def logEntry = logger.allLoggingEvents.first()
            logEntry.level == level
            if (throwable)
                assert logEntry.throwable.orNull() != null

        where:
            event                           | level | throwable
            messageBrokerStartedEvent       | DEBUG | false
            messageBrokerStoppedEvent       | DEBUG | false
            messageQueueCreatedEvent        | DEBUG | false
            messagePublishedEvent           | DEBUG | false
            pollingForMessagesEvent         | TRACE | false
            consumingNewMessageEvent        | DEBUG | false
            consumingTimedOutMessageEvent   | INFO  | false
            retryingMessageConsumptionEvent | WARN  | false
            messageConsumptionFailedEvent   | ERROR | true
            messageKeptAliveEvent           | TRACE | false
            messageConsumedEvent            | DEBUG | false
    }

    Event getMessageBrokerStartedEvent() {
        new MessageBrokerStartedEvent(messageBroker)
    }

    Event getMessageBrokerStoppedEvent() {
        new MessageBrokerStoppedEvent(messageBroker)
    }

    Event getMessageQueueCreatedEvent() {
        new MessageQueueCreatedEvent('queue id', [])
    }

    Event getMessagePublishedEvent() {
        new MessagePublishedEvent('queue id', 'a message')
    }

    Event getPollingForMessagesEvent() {
        new PollingForMessagesEvent([:])
    }

    Event getConsumingNewMessageEvent() {
        new ConsumingNewMessageEvent(update, 'a message')
    }

    Event getConsumingTimedOutMessageEvent() {
        new ConsumingTimedOutMessageEvent(update, 'a message')
    }

    Event getRetryingMessageConsumptionEvent() {
        new RetryingMessageConsumptionEvent(update, 'a message', new RuntimeException())
    }

    Event getMessageConsumptionFailedEvent() {
        new MessageConsumptionFailedEvent(update, 'a message', new RuntimeException())
    }

    Event getMessageKeptAliveEvent() {
        new MessageKeptAliveEvent(update, 'a message')
    }

    Event getMessageConsumedEvent() {
        new MessageConsumedEvent(update, 'a message')
    }
}
