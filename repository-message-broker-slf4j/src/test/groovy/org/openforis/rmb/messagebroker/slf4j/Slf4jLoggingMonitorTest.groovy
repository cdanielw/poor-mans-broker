package org.openforis.rmb.messagebroker.slf4j

import org.openforis.rmb.messagebroker.MessageBrokerConfig
import org.openforis.rmb.messagebroker.MessageConsumer
import org.openforis.rmb.messagebroker.MessageHandler
import org.openforis.rmb.messagebroker.RepositoryMessageBroker
import org.openforis.rmb.messagebroker.inmemory.InMemoryMessageRepository
import org.openforis.rmb.messagebroker.monitor.*
import org.openforis.rmb.messagebroker.spi.MessageDetails
import org.openforis.rmb.messagebroker.spi.MessageProcessingStatus
import org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate
import spock.lang.Specification
import spock.lang.Unroll
import uk.org.lidalia.slf4jtest.TestLogger
import uk.org.lidalia.slf4jtest.TestLoggerFactory

import static org.openforis.rmb.messagebroker.spi.MessageProcessingUpdate.Status.PENDING
import static org.openforis.rmb.messagebroker.spi.TransactionSynchronizer.NULL_TRANSACTION_SYNCHRONIZER
import static uk.org.lidalia.slf4jext.Level.*

class Slf4jLoggingMonitorTest extends Specification {
    static messageBroker = new RepositoryMessageBroker(
            MessageBrokerConfig.builder(new InMemoryMessageRepository(), NULL_TRANSACTION_SYNCHRONIZER)
    )
    static consumer = MessageConsumer.builder('consumer id', {} as MessageHandler).build()
    static update = MessageProcessingUpdate.take(consumer,
            new MessageDetails('queue id', 'message id', 0),
            new MessageProcessingStatus(PENDING, 0, null))

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
            event                                        | level | throwable
            messageBrokerStartedEvent                    | DEBUG | false
            messageBrokerStoppedEvent                    | DEBUG | false
            messageQueueCreatedEvent                     | DEBUG | false
            pollingForTimedOutMessagesFailedEvent        | ERROR | true
            pollingForMessageQueueSizeChangesFailedEvent | ERROR | true
            messagePublishedEvent                        | DEBUG | false
            pollingForMessagesEvent                      | TRACE | false
            consumingNewMessageEvent                     | DEBUG | false
            consumingTimedOutMessageEvent                | INFO  | false
            throttlingMessageRetryEvent                  | DEBUG | false
            retryingMessageConsumptionEvent              | WARN  | false
            messageConsumptionFailedEvent                | ERROR | true
            messageKeptAliveEvent                        | DEBUG | false
            messageConsumedEvent                         | DEBUG | false
            messageUpdateConflictEvent                   | ERROR | false
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

    Event getPollingForTimedOutMessagesFailedEvent() {
        new PollingForTimedOutMessagesFailedEvent(new RuntimeException())
    }

    Event getPollingForMessageQueueSizeChangesFailedEvent() {
        new PollingForMessageQueueSizeChangesFailedEvent(new RuntimeException())
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

    Event getThrottlingMessageRetryEvent() {
        new ThrottlingMessageRetryEvent(update, 'a message', new RuntimeException())
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

    Event getMessageUpdateConflictEvent() {
        new MessageUpdateConflictEvent(update, 'a message')
    }
}
